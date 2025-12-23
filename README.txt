%md
# Incremental Data Warehouse ETL Pipeline (Apache Spark on Azure Databricks)

## Overview

This repository contains a sample ETL pipeline implemented in PySpark for Azure Databricks. The pipeline demonstrates best practices for incremental data loading, transformation, and dimensional modeling in a modern data warehouse environment.

## Features

- **Incremental Loads:** Efficiently loads only new or changed data from source tables, keeping the latest available date and month-end snapshots.
- **Fact and Dimension Table Creation:** Transforms raw data into star-schema fact and dimension tables for analytics and reporting.
- **Data Quality Checks:** Includes logic to detect when no new data is available, preventing unnecessary processing.
- **Modular and Reusable:** Uses generic table and column names, making it easy to adapt to other domains or data models.

## Workflow

1. **Incremental Load:**  
   Each source table is loaded incrementally, combining the latest daily data with month-end snapshots to optimize storage and performance.

2. **Data Quality Check:**  
   The pipeline checks for differences between the last loaded data and the current data. If no changes are detected (and it's a weekday), the pipeline exits early.

3. **Fact Table Transformation:**  
   Joins and transforms the incremental data into a central fact table, applying business rules and creating surrogate keys.

4. **Dimension Table Creation:**  
   Builds dimension tables (e.g., Dealer, Equipment, Dates) from the incremental data, ensuring referential integrity and rich context for analytics.

5. **Output:**  
   All tables are written back to the data lake or warehouse in overwrite mode, ready for downstream consumption.

## Technologies Used

- **Apache Spark (PySpark)**
- **Azure Databricks**
- **SQL and DataFrame APIs**

## How to Use

1. Clone this repository and import the notebook into your Azure Databricks workspace.
2. Update the source and target table names to match your environment.
3. Run the notebook on a Databricks cluster (Spark 3.x+ recommended).
4. Review and adapt the transformation logic as needed for your data model.

## Customization

- Replace the generic table and column names with those relevant to your business domain.
- Adjust the incremental load logic to fit your data refresh patterns.
- Extend the fact and dimension transformations to include additional business rules or attributes.

## License

This project is provided for educational and portfolio purposes. Feel free to adapt and reuse for your own data engineering projects.

---

**Contact:**  
For questions or collaboration, please reach out via GitHub Issues or Pull Requests.