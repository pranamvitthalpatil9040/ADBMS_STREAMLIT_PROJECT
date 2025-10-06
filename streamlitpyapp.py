import streamlit as st
import pandas as pd
from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import plotly.express as px

# --- Configuration ---
MONGO_URI ="mongodb+srv://pranamvitthalpatil9040:pranam%409040@cluster0.fju3io1.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "UniversityDB_ADBMS"
COLLECTION_NAME = "students"

# --- MongoDB Connection Function ---
@st.cache_resource(show_spinner=False)
def init_connection():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # 1. Implementation of Indexing (Unique Student ID)
        collection.create_index("student_id", unique=True)
        
        return client, db, collection
    except errors.ConnectionError:
        st.error("❌ MongoDB Connection Failed! Ensure MongoDB Server is running on localhost:27017.")
        st.stop()
    except errors.OperationFailure as e:
        if "E11000 duplicate key error" in str(e):
             # This means index was successfully created on a previous run
             return client, db, collection
        else:
             st.error(f"MongoDB Operation Error: {e}")
             st.stop()

# Initialize connection and get collection object
client, db, students_collection = init_connection()

# --- Utility Functions ---

def fetch_all_students(department_filter=None):
    if department_filter:
        query = {"department": {"$in": department_filter}}
    else:
        query = {}
    
    data = list(students_collection.find(query).sort("name", 1))
    
    # Convert MongoDB IDs for easier handling
    for item in data:
        item['_id'] = str(item['_id'])
    
    return pd.DataFrame(data)

def calculate_gpa_value(grade):
    # Mapping for Aggregation/Reporting
    if grade == 'A': return 4.0
    if grade == 'B': return 3.0
    if grade == 'C': return 2.0
    if grade == 'D': return 1.0
    return 0.0 # F or other invalid grades

# --- ADBMS Implementation Functions ---

# 2. Aggregation Report: AVG GPA by Department
def run_gpa_aggregation(departments_to_include):
    # This report runs the complex aggregation pipeline
    pipeline = [
        # Match only students in the selected departments (for interactivity)
        {"$match": {"department": {"$in": departments_to_include}}},
        
        {"$unwind": "$courses_and_grades"}, # Flatten the grades array
        
        {"$project": {
            "department": 1,
            # Use $switch to convert letter grade to numerical GPA value
            "grade_value": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$courses_and_grades.grade", "A"]}, "then": 4},
                        {"case": {"$eq": ["$courses_and_grades.grade", "B"]}, "then": 3},
                        {"case": {"$eq": ["$courses_and_grades.grade", "C"]}, "then": 2},
                        {"case": {"$eq": ["$courses_and_grades.grade", "D"]}, "then": 1},
                    ],
                    "default": 0
                }
            }
        }},
        
        {"$group": {
            "_id": "$department",
            "average_gpa": {"$avg": "$grade_value"}, # Calculate the average
        }},
        {"$sort": {"average_gpa": -1}}
    ]

    report = list(students_collection.aggregate(pipeline))
    return pd.DataFrame(report) if report else pd.DataFrame(columns=['_id', 'average_gpa'])

# 3. Map-Reduce (Group Count Simulation)
def run_student_count_report(departments_to_include):
    # Using Aggregation for Group Count, as it is the performant replacement for Map-Reduce
    pipeline = [
        # Match only students in the selected departments (for interactivity)
        {"$match": {"department": {"$in": departments_to_include}}},
        
        {"$group": { 
            "_id": "$department",
            "student_count": {"$sum": 1}
        }},
        {"$sort": {"student_count": -1}}
    ]
    report = list(students_collection.aggregate(pipeline))
    return pd.DataFrame(report) if report else pd.DataFrame(columns=['_id', 'student_count'])

# --- Streamlit UI Layout ---

st.set_page_config(layout="wide", page_title="ADBMS University Data Hub")

# Sidebar for Interaction
st.sidebar.title("Configuration & Filters")

all_departments = sorted(list(students_collection.distinct("department")))
if not all_departments:
    all_departments = ['CS', 'IT', 'ELEX'] # Default list if DB is empty

selected_departments = st.sidebar.multiselect(
    "Filter Reports by Department",
    options=all_departments,
    default=all_departments,
    key='dept_filter'
)

st.sidebar.markdown("---")
st.sidebar.caption("Project: ADBMS Mini-Project (MongoDB)")
st.sidebar.caption("Concepts: CRUD, Indexing, Aggregation")

# Main Content
st.title("🎓 University Data Hub")
st.markdown("---")

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["Dashboard & Reports", "Add/Edit Records (CRUD)", "Debugging & Data Viewer"])

# --- Tab 1: Dashboard and Reports ---
with tab1:
    
    st.header("Analytical Dashboard")
    
    # Fetch Data for KPIs and Charts (Filtered by sidebar)
    df_filtered = fetch_all_students(selected_departments)
    
    # 1. KPI Cards
    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
    
    total_students = len(df_filtered)
    total_departments = len(df_filtered['department'].unique()) if not df_filtered.empty else 0
    
    # Calculate global average GPA for KPI card (requires complex logic)
    df_gpa = run_gpa_aggregation(selected_departments)
    if not df_gpa.empty:
        total_avg_gpa = df_gpa['average_gpa'].mean().round(2)
        highest_gpa_dept = df_gpa.iloc[0]['_id']
    else:
        total_avg_gpa = 0.0
        highest_gpa_dept = "N/A"

    with col_kpi1:
        st.metric("Total Students (Filtered)", total_students)
    with col_kpi2:
        st.metric("Departments Represented", total_departments)
    with col_kpi3:
        st.metric("Average GPA (Overall)", total_avg_gpa)
    with col_kpi4:
        st.metric("Top Performing Dept.", highest_gpa_dept)

    st.markdown("---")

    # 2. Aggregation Report Chart (AVG GPA)
    st.subheader("Report 1: Average GPA by Department")
    if not df_gpa.empty:
        fig_gpa = px.bar(
            df_gpa, 
            x='_id', 
            y='average_gpa', 
            title='Average GPA by Department (A=4.0)',
            labels={'_id': 'Department', 'average_gpa': 'Average GPA'},
            color='average_gpa',
            color_continuous_scale=px.colors.sequential.Teal
        )
        st.plotly_chart(fig_gpa, use_container_width=True)
    else:
        st.info("No data available for GPA Aggregation Report.")

    st.markdown("---")
    
    # 3. Map-Reduce (Student Count Report Chart)
    st.subheader("Report 2: Student Count per Department")
    df_count = run_student_count_report(selected_departments)
    
    if not df_count.empty:
        fig_count = px.pie(
            df_count,
            names='_id',
            values='student_count',
            title='Student Distribution Across Departments',
            color_discrete_sequence=px.colors.sequential.Agsunset
        )
        st.plotly_chart(fig_count, use_container_width=True)
    else:
        st.info("No data available for Student Count Report.")

# --- Tab 2: Add/Edit Records (CRUD) ---
with tab2:
    st.header("Student Management (CRUD Operations)")

    # 1. CREATE Student Form
    st.subheader("1. Add New Student")
    with st.form("create_student_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            student_id = st.text_input("Student ID (Unique)", key="new_sid").upper()
            department = st.text_input("Department", key="new_dept")
        
        with col2:
            name = st.text_input("Name", key="new_name")
            initial_grade = st.selectbox(
                "Initial Grade",
                options=['A', 'B', 'C', 'D', 'F'],
                key="new_grade"
            )

        submitted = st.form_submit_button("💾 Save Student (CREATE)")

        if submitted:
            try:
                if not student_id or not name or not department:
                    st.error("All fields must be filled.")
                else:
                    new_student = {
                        "student_id": student_id,
                        "name": name,
                        "department": department,
                        "courses_and_grades": [{"course": "Intro", "grade": initial_grade}],
                        "created_at": pd.Timestamp.now()
                    }
                    
                    # C: CREATE operation
                    students_collection.insert_one(new_student)
                    st.success(f"Successfully added student: {name} (ID: {student_id})")
                    st.rerun() # Refresh app to update list

            except errors.DuplicateKeyError:
                # Indexing constraint violation handled here
                st.error(f"❌ Error: Student ID '{student_id}' already exists (Unique Index Violation).")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")

    st.markdown("---")

    # 2. READ/UPDATE/DELETE Section
    st.subheader("2. View and Edit Records")

    # R: READ operation
    df_all = fetch_all_students()
    
    if not df_all.empty:
        # Create a dictionary mapping Name to MongoDB _id
        id_map = df_all.set_index('student_id')['_id'].to_dict()
        
        selected_sid = st.selectbox(
            "Select Student to Edit/Delete",
            options=[""] + df_all['student_id'].tolist(),
            key='select_sid'
        )
        
        if selected_sid:
            student_data = df_all[df_all['student_id'] == selected_sid].iloc[0]
            mongo_id = id_map[selected_sid]
            
            st.markdown(f"#### Editing Student: **{selected_sid}**")
            
            with st.form("edit_student_form"):
                edit_name = st.text_input("Name", value=student_data['name'], key="edit_name")
                edit_department = st.text_input("Department", value=student_data['department'], key="edit_dept")

                col_edit, col_delete = st.columns([1, 1])
                
                # U: UPDATE operation
                update_btn = col_edit.form_submit_button("🔄 Update Record (UPDATE)")
                
                # D: DELETE operation
                delete_btn = col_delete.form_submit_button("🗑️ Delete Record (DELETE)", type="primary")

                if update_btn:
                    update_result = students_collection.update_one(
                        {"_id": ObjectId(mongo_id)},
                        {"$set": {"name": edit_name, "department": edit_department}}
                    )
                    if update_result.modified_count > 0:
                        st.success(f"Student {selected_sid} updated successfully!")
                        st.rerun()
                    else:
                        st.warning("No changes detected or update failed.")

                if delete_btn:
                    if st.warning(f"Are you sure you want to permanently delete {selected_sid}?"):
                        delete_result = students_collection.delete_one({"_id": ObjectId(mongo_id)})
                        if delete_result.deleted_count > 0:
                            st.error(f"Student {selected_sid} deleted.")
                            st.rerun()
                        else:
                            st.error("Delete failed.")
    else:
        st.info("No students found in the database. Add one above!")


# --- Tab 3: Debugging and Data Viewer ---
with tab3:
    st.header("Raw Data Viewer")
    st.markdown("Use this tab to inspect the raw MongoDB collection data.")
    
    # R: READ raw data
    df_all = fetch_all_students()
    
    if not df_all.empty:
        st.dataframe(df_all)
    else:

        st.info("Database is currently empty.")
