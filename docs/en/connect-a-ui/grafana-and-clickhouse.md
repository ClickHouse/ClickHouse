---
sidebar_label: Grafana
sidebar_position: 75
keywords: [clickhouse, grafana, connect, integrate]
description: With Grafana you can create, explore and share all of your data through dashboards.
---

# Connecting Grafana to ClickHouse

With Grafana you can create, explore and share all of your data through dashboards. Grafana requires a plugin to connect to ClickHouse, which is easily installed within their UI. 

## 1.  Install the Grafana Plugin for ClickHouse

Before Grafana can talk to ClickHouse, you need to install the appropriate Grafana plugin. Assuming you are logged in to Grafana, follow these steps:

1. From the **Configuration** page, select the **Plugins** tab.

2. Search for **ClickHouse** and click on the **Signed** plugin by Grafana Labs:

    <img src={require('./images/grafana_01.png').default} class="image" alt="Select the ClickHouse plugin" />

3. On the next screen, click the **Install** button:

    <img src={require('./images/grafana_02.png').default} class="image" alt="Install the ClickHouse plugin" />

## 2. Define a ClickHouse data source

1. Once the installation is complete, click the **Create a ClickHouse data source** button. (You can also add a data source from the **Data sources** tab on the **Configuration** page.)

    <img src={require('./images/grafana_03.png').default} class="image" alt="Create a ClickHouse data source" />

2. Either scroll down and find the **ClickHouse** data source type, or you can search for it in the search bar of the **Add data source** page. Either way, select the **ClickHouse** data source type and the following dialog appears:

    <img src={require('./images/grafana_04.png').default} class="image" alt="Add data source" />

3. Enter your server settings and credentials. The key settings are:

- **Name:** a Grafana setting - give your data source any name you like 
- **Server address:** the URL of your ClickHouse service
- **Server port:** 9000 for unsecure, 9440 for secure (unless you modified the ClickHouse ports)
- **Username** and **Password**: enter your ClickHouse user credentials. If you have not configured users and passwords, then try **default** for the username and leave the password empty.
- **Default database:** a Grafana setting - you can specify a database that Grafana defaults to when using this data source (this property can be left blank)

4. Click the **Save & test** button to verify that Grafana can connect to your ClickHouse service. If successful, you will see a **Data source is working** message:

    <img src={require('./images/grafana_05.png').default} class="image" alt="Select Save & test" />

## 3. Build a dashboard

1. From the left menu, click on the **Dashboards** icon and select **Browse**. Then select the **New Dashboard** button: 

    <img src={require('./images/grafana_06.png').default} class="image" alt="New Dashboard" />

2. Click the **Add a new panel** button.

3. From here, you can build a visualization based on a query. From the **Data source** dropdown, select your ClickHouse data source that you defined earlier. Then you can either use the **Query Builder** to build a query visually, or switch to the **SQL Editor** and enter a SQL query (as shown here):

    <img src={require('./images/grafana_07.png').default} class="image" alt="Run a SQL Query" />

4. That's it! You are now ready to <a href="https://grafana.com/docs/grafana/latest/visualizations/" target="_blank">build visualizations</a> and <a href="https://grafana.com/docs/grafana/latest/dashboards/" target="_blank">dashboards</a> in Grafana.