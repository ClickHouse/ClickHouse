---
sidebar_label: Vector
sidebar_position: 220
description: How to tail a log file into ClickHouse using Vector
---

# Integrating Vector with ClickHouse

Being able to analyze your logs in real time is critical for production applications. Have you ever wondered if ClickHouse is good at storing and analyzing log data? Just checkout <a href="https://eng.uber.com/logging/" target="_blank">Uber's experience</a> with converting their logging infrastructure from ELK to ClickHouse. 

This guide shows how to use the popular data pipeline <a href="https://vector.dev/docs/about/what-is-vector/" target="_blank">Vector</a> to tail an Nginx log file and send it to ClickHouse. The steps below would be similar for tailing any type of log file. We will assume you already have ClickHouse up and running and Vector installed (no need to start it yet though).

## 1. Create a database and table

Let's define a table to store the log events:

1. We will start with a new database named **nginxdb**:
    ```sql
    CREATE DATABASE IF NOT EXISTS nginxdb
    ```

2. For starters, we are just going to insert the entire log event as a single string. Obviously this is not a great format for performing analytics on the log data, but we will figure that part out below using ***materialized views***.
    ```sql
    CREATE TABLE IF NOT EXISTS  nginxdb.access_logs (
        message String
    ) 
    ENGINE = MergeTree()
    ORDER BY tuple()
    ```
    :::note
    There is not really a need for a primary key yet, so that is why **ORDER BY** is set to **tuple()**.
    :::


## 2.  Configure Nginx

We certainly do not want to spend too much time explaining Nginx, but we also do not want to hide all the details, so in this step we will provide you with enough details to get Nginx logging configured. 


1. The following `access_log` property sends logs to **/var/log/nginx/my_access.log** in the **combined** format. This value goes in the `http` section of your **nginx.conf** file:
    ```bash
    http {
        include       /etc/nginx/mime.types;
        default_type  application/octet-stream;
        access_log  /var/log/nginx/my_access.log combined;
        sendfile        on;
        keepalive_timeout  65;
        include /etc/nginx/conf.d/*.conf;
    }
    ```

2. Be sure to restart Nginx if you had to modify **nginx.conf**.

3. Generate some log events in the access log by visiting pages on your web server. Logs in the **combined** format have the following format:
    ```bash
    192.168.208.1 - - [12/Oct/2021:03:31:44 +0000] "GET / HTTP/1.1" 200 615 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    192.168.208.1 - - [12/Oct/2021:03:31:44 +0000] "GET /favicon.ico HTTP/1.1" 404 555 "http://localhost/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    192.168.208.1 - - [12/Oct/2021:03:31:49 +0000] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    ``` 

## 3. Configure Vector

Vector collects, transforms and routes logs, metrics, and traces (referred to as **sources**) to lots of different vendors (referred to as **sinks**), including out-of-the-box compatibility with ClickHouse. Sources and sinks are defined in a configuration file named **vector.toml**.


1. The following **vector.toml** defines a **source** of type **file** that tails the end of **my_access.log**, and it also defines a **sink** as the **access_logs** table defined above:
    ```bash
    [sources.nginx_logs]
    type = "file"
    include = [ "/var/log/nginx/my_access.log" ]
    read_from = "end"

    [sinks.clickhouse]
    type = "clickhouse"
    inputs = ["nginx_logs"]
    endpoint = "http://clickhouse-server:8123"
    database = "nginxdb"
    table = "access_logs"
    skip_unknown_fields = true
    ```

2. Start up Vector using the configuration above. <a href="https://vector.dev/docs/" target="_blank">Visit the Vector documentation</a> for more details on defining sources and sinks.

3. Verify the access logs are being inserted into ClickHouse. Run the following query and you should see the access logs in your table:
    ```sql
    SELECT * FROM nginxdb.access_logs
    ```
    <img src={require('./images/vector_01.png').default} class="image" alt="View the logs" />


## 4. Parse the Logs

Having the logs in ClickHouse is great, but storing each event as a single string does not allow for much data analysis. Let's see how to parse the log events using a materialized view.


1. A **materialized view** (MV, for short) is a new table based on an existing table, and when inserts are made to the existing table, the new data is also added to the materialized view. Let's see how to define a MV that contains a parsed representation of the log events in **access_logs**, in other words:
    ```bash
    192.168.208.1 - - [12/Oct/2021:15:32:43 +0000] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    ```

    There are various functions in ClickHouse to parse the string, but for starters let's take a look at **splitByWhitespace** - which parses a string by whitespace and returns each token in an array. To demonstrate, run the following command:
    ```sql
    SELECT splitByWhitespace('192.168.208.1 - - [12/Oct/2021:15:32:43 +0000] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"')
    ```

    Notice the response is pretty close to what we want! A few of the strings have some extra characters, and the user agent (the browser details) did not need to be parsed, but we will resolve that in the next step:
    ```
    ["192.168.208.1","-","-","[12/Oct/2021:15:32:43","+0000]","\"GET","/","HTTP/1.1\"","304","0","\"-\"","\"Mozilla/5.0","(Macintosh;","Intel","Mac","OS","X","10_15_7)","AppleWebKit/537.36","(KHTML,","like","Gecko)","Chrome/93.0.4577.63","Safari/537.36\""]
    ```

2. Similar to **splitByWhitespace**, the **splitByRegexp** function splits a string into an array based on a regular expression. Run the following command, which returns two strings. 
    ```sql
    SELECT splitByRegexp('\S \d+ "([^"]*)"', '192.168.208.1 - - [12/Oct/2021:15:32:43 +0000] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"')
    ```

    Notice the second string returned is the user agent successfully parsed from the log:
    ```
    ["192.168.208.1 - - [12/Oct/2021:15:32:43 +0000] \"GET / HTTP/1.1\" 30"," \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36\""]
    ```

3. Before looking at the final **CREATE MATERIALIZED VIEW** command, let's view a couple more functions used to cleanup the data. For example, the **RequestMethod** looks like **"GET** with an unwanted double-quote. Run the following **trim** function, which removes the double quote:
    ```sql
    SELECT trim(LEADING '"' FROM '"GET')
    ```

4. The time string has a leading square bracket, and also is not in a format that ClickHouse can parse into a date. However, if we change the separater from a colon (**:**) to a comma (**,**) then the parsing works great:
    ```sql
    SELECT parseDateTimeBestEffort(replaceOne(trim(LEADING '[' FROM '[12/Oct/2021:15:32:43'), ':', ' '))
    ```

5. We are now ready to define our materialized view. Our definition includes **POPULATE**, which means the existing rows in **access_logs** will be processed and inserted right away. Run the following SQL statement:
    ```sql
    CREATE MATERIALIZED VIEW nginxdb.access_logs_view
    (
        RemoteAddr String,
        Client String,
        RemoteUser String,
        TimeLocal DateTime,
        RequestMethod String,
        Request String,
        HttpVersion String,
        Status Int32,
        BytesSent Int64,
        UserAgent String
    )
    ENGINE = MergeTree()
    ORDER BY RemoteAddr
    POPULATE AS
    WITH 
        splitByWhitespace(message) as split,
        splitByRegexp('\S \d+ "([^"]*)"', message) as referer
    SELECT
        split[1] AS RemoteAddr,
        split[2] AS Client,
        split[3] AS RemoteUser,
        parseDateTimeBestEffort(replaceOne(trim(LEADING '[' FROM split[4]), ':', ' ')) AS TimeLocal,
        trim(LEADING '"' FROM split[6]) AS RequestMethod,
        split[7] AS Request,
        trim(TRAILING '"' FROM split[8]) AS HttpVersion,
        split[9] AS Status,
        split[10] AS BytesSent,
        trim(BOTH '"' from referer[2]) AS UserAgent
    FROM 
        (SELECT message FROM nginxdb.access_logs)
    ```

6. Now verify it worked. You should see the access logs nicely parsed into columns:
    ```sql
    SELECT * FROM nginxdb.access_logs_view
    ```
    <img src={require('./images/vector_02.png').default} class="image" alt="View the logs" />

    :::note
    The lesson above stored the data in two tables, but you could change the initial **nginxdb.access_logs** table to use the **Null** table engine - the parsed data will still end up in the **nginxdb.access_logs_view** table, but the raw data will not be stored in a table.
    :::


**Summary:** By using Vector, which only required a simple install and quick configuration, we can send logs from an Nginx server to a table in ClickHouse. By using a clever materialized view, we can parse those logs into columns for easier analytics. 
