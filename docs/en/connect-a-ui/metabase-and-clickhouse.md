---
sidebar_label: Metabase
sidebar_position: 131
keywords: [clickhouse, metabase, connect, integrate, ui]
description: Metabase is an easy-to-use, open source UI tool for asking questions about your data.
---

# Connecting Metabase to ClickHouse

Metabase is an easy-to-use, open source UI tool for asking questions about your data. Metabase is a Java application that can be executed by simply <a href="https://www.metabase.com/start/oss/jar" target="_blank">downloading the JAR file</a> and running it with `java -jar metabase.jar`. Metabase connects to ClickHouse using a JDBC driver that you download and put in the `plugins` folder:

## 1.  Download the ClickHouse plugin for Metabase

1. If you do not have a `plugins` folder, create one as a subfolder of where you have `metabase.jar` saved.

2. The plugin is a JAR file named `clickhouse.metabase-driver.jar`. Download the latest version of the JAR file at <a href="https://github.com/enqueue/metabase-clickhouse-driver/release" target="_blank">https://github.com/enqueue/metabase-clickhouse-driver/releases/latest</a>

3. Save `clickhouse.metabase-driver.jar` in your `plugins` folder.

4. Start (or restart) Metabase so that the driver gets loaded properly.

5. Access Metabse at <a href="http://localhost:3000/" target="_blank">http://hostname:3000</a>. On the initial startup, you will see a welcome screen and have to work your way through a list of questions. If prompted to select a database, select "**I'll add my data later**":


## 2.  Connect Metabase to ClickHouse

1. Click on the gear icon in the top-right corner and select **Admin Settings** to visit your <a href="http://localhost:3000/admin/settings/setup" target="_blank">Metabase admin page</a>.

2. Click on **Add a database**. Alternately, you can click on the **Databases** tab and select the **Add database** button.

3. If your driver installation worked, you will see **ClickHouse** in the dropdown menu for **Database type**:

    <img src={require('./images/metabase_01.png').default} class="image" alt="Add a ClickHouse database" />

4. Give your database a **Display name**, which is a Metabase setting - so use any name you like.

5. Enter the connection details of your ClickHouse database. For example:

    <img src={require('./images/metabase_02.png').default} class="image" style={{width: '80%'}}  alt="Connection details" />

6. Click the **Save** button and Metabase will scan your database for tables. 

## 3. Run a SQL query

1. Exit the **Admin settings** by clicking the **Exit admin** button in the top-right corner.

2. In the top-right corner, click the **+ New** menu and notice you can ask questions, run SQL queries, and build a dashboard:

    <img src={require('./images/metabase_03.png').default} class="image" style={{width: 283}} alt="New menu" />

3. For example, here is a SQL query executed on a table named `hits` that returns the top ten most-visited URLs:

    <img src={require('./images/metabase_04.png').default} class="image" alt="Run a SQL query" />

## 4. Ask a question

1. Click on **+ New** and select **Question**. Notice you can build a question by starting wtih a database and table. For example, the following question is being asked of a table named `hits` in the `Web Traffic Database`:

    <img src={require('./images/metabase_05.png').default} class="image" alt="New question" />


2. Here is a simple question that calculates the top 20 most-visited URLs in the table:

    <img src={require('./images/metabase_06.png').default} class="image" alt="New question" />

3. Click the **Visualize** button to see the results in a tabular view.

4. Below the results, click the **Visualization** button to change the visualization to a bar chart (or any of the other options avaialable):

    <img src={require('./images/metabase_07.png').default} class="image" alt="New question" />

5. Find more information about Metabase and how to build dashboards by <a href="https://www.metabase.com/docs/latest/" target="_blank">visiting the Metabase documentation</a>.