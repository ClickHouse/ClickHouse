---
sidebar_label: JDBC for External Data Sources
sidebar_position: 200
keywords: [clickhouse, jdbc, connect, integrate]
description: The ClickHouse JDBC Bridge allows ClickHouse to access data from any external data source for which a JDBC driver is available
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connecting ClickHouse to external data sources with JDBC

**Overview:** The <a href="https://github.com/ClickHouse/clickhouse-jdbc-bridge" target="_blank">ClickHouse JDBC Bridge</a> in combination with the <a href="https://clickhouse.com/docs/en/sql-reference/table-functions/jdbc/" target="_blank">jdbc Table Function</a> or the <a href="https://clickhouse.com/docs/en/engines/table-engines/integrations/jdbc/" target="_blank">JDBC Table Engine</a> allows ClickHouse to access data from any external data source for which a <a href="https://en.wikipedia.org/wiki/JDBC_driver" target="_blank">JDBC driver</a> is available:
<img src={require('./images/jdbc-01.png').default} class="image" alt="ClickHouse JDBC Bridge"/>
This is handy when there is no native built-in <a href="https://clickhouse.com/docs/en/engines/table-engines/#integration-engines" target="_blank">integration engine</a>, <a href="https://clickhouse.com/docs/en/sql-reference/table-functions/" target="_blank">table function</a>, or <a href="https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources/" target="_blank">external dictionary</a> for the external data source available, but a JDBC driver for the data source exists. 

You can use the ClickHouse JDBC Bridge for both reads and writes. And in parallel for multiple external data sources, e.g. you can run distributed queries on ClickHouse across multiple external and internal data sources in real time.

In this lesson we will show you how easy it is to install, configure, and run the ClickHouse JDBC Bridge in order to connect ClickHouse with an external data source. We will use MySQL as the external data source for this lesson.

Let's get started!





:::note Prerequisites
You have access to a machine that has:
1. a Unix shell and internet access 
2. <a href="https://www.gnu.org/software/wget/" target="_blank">wget</a> installed
3. a current version of **Java** (e.g. <a href="https://openjdk.java.net" target="_blank">OpenJDK</a> Version >= 17) installed
4. a current version of **MySQL** (e.g. <a href="https://www.mysql.com" target="_blank">MySQL</a> Version >=8) installed and running
5. a current version of **ClickHouse** <a href="https://clickhouse.com/docs/en/getting-started/install/" target="_blank">installed</a> and running
::: 

## Install the ClickHouse JDBC Bridge locally

The easiest way to use the ClickHouse JDBC Bridge is to install and run it on the same host where also ClickHouse is running:<img src={require('./images/jdbc-02.png').default} class="image" alt="ClickHouse JDBC Bridge locally"/>

Let's start by connecting to the Unix shell on the machine where ClickHouse is running and create a local folder where we will later install the ClickHouse JDBC Bridge into (feel free to name the folder anything you like and put it anywhere you like):
```bash
mkdir ~/clickhouse-jdbc-bridge
```



Now we download the <a href="https://github.com/ClickHouse/clickhouse-jdbc-bridge/releases/" target="_blank">current version</a> of the ClickHouse JDBC Bridge into that folder:
```bash
cd ~/clickhouse-jdbc-bridge
wget https://github.com/ClickHouse/clickhouse-jdbc-bridge/releases/download/v2.0.7/clickhouse-jdbc-bridge-2.0.7-shaded.jar
```
 

   
In order to be able to connect to MySQL we are creating a named data source:



 ```bash
 cd ~/clickhouse-jdbc-bridge
 mkdir -p config/datasources
 touch config/datasources/mysql8.json
 ```
   
 You can now copy and paste the following configuration into the file `~/clickhouse-jdbc-bridge/config/datasources/mysql8.json`:
 ```json
 {
   "mysql8": {
   "driverUrls": [
     "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar"
   ],
   "jdbcUrl": "jdbc:mysql://<host>:<port>",
   "username": "<username>",
   "password": "<password>"
   }
 }
 ```
   
   
:::note
in the config file above
- you are free to use any name you like for the datasource, we used `mysql8`
- in the value for the `jdbcUrl` you need to replace `<host>`, and `<port>` with appropriate values according to your running MySQL instance, e.g. `"jdbc:mysql://localhost:3306"`
- you need to replace `<username>` and `<password>` with your MySQL credentials, if you don't use a password, you can delete the `"password": "<password>"` line in the config file above
- in the value for `driverUrls` we just specified a URL from which the <a href="https://repo1.maven.org/maven2/mysql/mysql-connector-java/" target="_blank">current version</a> of the MySQL JDBC driver can be downloaded. That's all we have to do, and the ClickHouse JDBC Bridge will automatically download that JDBC driver (into a OS specific directory).
:::
   

   
<br/>

Now we are ready to start the ClickHouse JDBC Bridge:
 ```bash
 cd ~/clickhouse-jdbc-bridge
 java -jar clickhouse-jdbc-bridge-2.0.7-shaded.jar
 ```
:::note
We started the ClickHouse JDBC Bridge in foreground mode. In order to stop the Bridge you can bring the Unix shell window from above in foreground and press `CTRL+C`. 
:::


## Use the JDBC connection from within ClickHouse

ClickHouse can now access MySQL data by either using the <a href="https://clickhouse.com/docs/en/sql-reference/table-functions/jdbc/" target="_blank">jdbc Table Function</a> or the <a href="https://clickhouse.com/docs/en/engines/table-engines/integrations/jdbc/" target="_blank">JDBC Table Engine</a>. 

The easiest way to execute the following examples is to copy and paste them into the <a href="https://clickhouse.com/docs/en/interfaces/cli/" target="_blank">native ClickHouse command-line client</a> or into the <a href="https://clickhouse.com/docs/en/interfaces/http/" target="_blank">ClickHouse play HTTP Interface</a>.



- jdbc Table Function:

 ```sql
 SELECT * FROM jdbc('mysql8', 'mydatabase', 'mytable');
 ```
:::note
As the first paramter for the jdbc table funtion we are using the name of the named data source that we configured above. 
:::  



- JDBC Table Engine:
 ```sql
 CREATE TABLE mytable (
      <column> <column_type>,
      ...
 )   
 ENGINE = JDBC('mysql8', 'mydatabase', 'mytable');

 SELECT * FROM mytable;
 ```
:::note
 As the first paramter for the jdbc engine clause we are using the name of the named data source that we configured above

 The schema of the ClickHouse JDBC engine table and schema of the connected MySQL table must be aligned, e.g. the column names and order must be the same, and the column data types must be compatible 
::: 





   

## Install the ClickHouse JDBC Bridge externally

For a distributed ClickHouse cluster (a cluster with more than one ClickHouse host) it makes sense to install and run the ClickHouse JDBC Bridge externally on its own host:
<img src={require('./images/jdbc-03.png').default} class="image" alt="ClickHouse JDBC Bridge externally"/>
This has the advantage that each ClickHouse host can access the JDBC Bridge. Otherwise the JDBC Bridge would need to be installed locally for each ClickHouse instance that is supposed to access external data sources via the Bridge.

In order to install the ClickHouse JDBC Bridge externally, we do the following steps:


1. We install, configure and run the ClickHouse JDBC Bridge on a dedicated host by following the steps described in section 1 of this guide.

2. On each ClickHouse Host we add the following configuration block to the <a href="https://clickhouse.com/docs/en/operations/configuration-files/#configuration_files" target="_blank">ClickHouse server configuration</a> (depending on your chosen configuration format, use either the XML or YAML version):

<Tabs>
<TabItem value="xml" label="XML">

```xml
<jdbc_bridge>
   <host>JDBC-Bridge-Host</host>
   <port>9019</port>
</jdbc_bridge>
```

</TabItem>
<TabItem value="yaml" label="YAML">

```yaml
jdbc_bridge:
    host: JDBC-Bridge-Host
    port: 9019
```

</TabItem>
</Tabs>

:::note
   - you need to replace `JDBC-Bridge-Host` with the hostname or ip address of the dedicated ClickHouse JDBC Bridge host
   - we specified the default ClickHouse JDBC Bridge port `9019`, if you are using a different port for the JDBC Bridge then you must adapt the configuration above accordingly 
:::




[//]: # (## 4. Additional Infos)

[//]: # ()
[//]: # (TODO: )

[//]: # (- mention that for jdbc table function it is more performant &#40;not two queries each time&#41; to also specify the schema as a parameter)

[//]: # ()
[//]: # (- mention adhoc query vs table query, saved query, named query)

[//]: # ()
[//]: # (- mention insert into )



