---
sidebar_label: Superset
sidebar_position: 198
keywords: [clickhouse, superset, connect, integrate, ui]
description: Apache Superset is an open-source data exploration and visualization platform.
---

# Connect Superset to ClickHouse

<a href="https://superset.apache.org/" target="_blank">Apache Superset</a> is an open-source data exploration and visualization platform written in Python. Superset connects to ClickHouse using a Python driver with a SQLAlchemy dialect. Let's see how it works...

## 1. Install the Drivers

1. Superset uses the `clickhouse-sqlalchemy` driver, which requires the `clickhouse-driver` to connect to ClickHouse. The details of `clickhouse-driver` are at <a href="https://pypi.org/project/clickhouse-driver/" target="_blank">https://pypi.org/project/clickhouse-driver/</a> and can be installed with the following command:

    ```bash
    pip install clickhouse-driver 
    ```

2. Now install the <a href="https://pypi.org/project/clickhouse-sqlalchemy/" target="_blank">ClickHouse SQLAlchemy driver</a>:

    ```bash
    pip install clickhouse-sqlalchemy
    ```

3. Start (or restart) Superset.

## 2. Connect Superset to ClickHouse

1. Within Superset, select **Data** from the top menu and then **Databases** from the drop-down menu. Add a new database by clicking the **+ Database** button:

    <img src={require('./images/superset_01.png').default} class="image" alt="Add a new database" />

2. In the first step, select **ClickHouse** as the type of database:

    <img src={require('./images/superset_02.png').default} class="image" alt="Select ClickHouse" />

3. In the second step, enter a display name for your database and the connection URI. The **DISPLAY NAME** can be any name you prefer. The **SQLALCHEMY URI** is the important setting - it has the following format:
    ```
    clickhouse+native://username:password@hostname/database_name
    ```

    In the example below, ClickHouse is running on **localhost** with the **default** user and no password. The name of the database is **covid19db**. Use the **TEST CONNECTION** button to verify that Superset is connecting to your ClickHouse database properly:
    
    <img src={require('./images/superset_03.png').default} class="image" alt="Test the connection" />

4. Click the **CONNECT** button to complete the setup wizard, and you should see your database in the list of databases.

## 3. Add a Dataset

1. To define new charts (visualizations) in Superset, you need to define a **_dataset_**. From the top menu in Superset, select **Data**, then **Datasets** from the drop-down menu. 

2. Click the button for adding a dataset. Select your new database as the datasource and you should see the tables defined in your database. For example, the **covid19db** database has a table named **daily_totals**:

    <img src={require('./images/superset_04.png').default} class="image" alt="New dataset" />


3. Click the **ADD** button at the bottom of the dialog window and your table appears in the list of datasets. You are ready to build a dashboard and analyze your ClickHouse data!


## 4.  Creating charts and a dashboard in Superset

If you are familiar with Superset, then you will feel right at home with this next section. If you are new to Superset, well...it's like a lot of the other cool visualization tools out there in the world - it doesn't take long to get started, but the details and nuances get learned over time as you use the tool. 


1. You start with a dashboard. From the top menu in Superset, select **Dashboards**. Click the button in the upper-right to add a new dashboard. The following dashboard is named **Covid-19 Dashboard**:

    <img src={require('./images/superset_05.png').default} class="image" alt="New dashboard" />

2. To create a new chart, select **Charts** from the top menu and click the button to add a new chart. You will be shown a lot of options. The following example shows a **Big Number** chart using the **daily_totals** dataset from the **CHOOSE A DATASET** drop-down:

    <img src={require('./images/superset_06.png').default} class="image" alt="New chart" />

3. You need to add a metric to a **Big Number**. The column named **DATA** and the section named **Query** with a **METRIC** field show a red warning because they are not defined yet. To add a metric, click **Add metric** and a small dialog window appears:

    <img src={require('./images/superset_07.png').default} class="image" style={{width: '70%'}}   alt="Add a metric" />

4. The following example uses the **SUM** metric, found on the the **SIMPLE** tab. It sums the values of the **new_cases** column:

    <img src={require('./images/superset_08.png').default} class="image" style={{width: '70%'}}  alt="The SUM metric" />

5. To view the actual number, click the **RUN QUERY** button:

    <img src={require('./images/superset_09.png').default} class="image" style={{width: '70%'}}  alt="Run the query" />

6. Click the **SAVE** button to save the chart, then select **Covid-19 Dashboard** under the **ADD TO DASHBOARD** drop-down, then **SAVE & GO TO DASHBOARD** saves the chart and adds it to the dashboard:

    <img src={require('./images/superset_10.png').default} class="image" style={{width: '70%'}}  alt="Add Chart to Dashboard" />

7. That's it. Building dashboards in Superset based on data in ClickHouse opens up a whole world of blazing fast data analytics!

    <img src={require('./images/superset_11.png').default} class="image" alt="New Dashboard" />
