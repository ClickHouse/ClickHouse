---
sidebar_label: Connect Java to ClickHouse
sidebar_position: 201
keywords: [clickhouse, jdbc, connect, integrate]
description: The ClickHouse JDBC driver enables a Java application to interact with ClickHouse
---

# Connecting Applications to ClickHouse with JDBC

**Overview:** The <a href="https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-jdbc" target="_blank">ClickHouse JDBC driver</a> enables a Java application to interact with ClickHouse:
<img src={require('./images/jdbc-2-01.png').default} class="image" alt="ClickHouse JDBC Driver"/>

In this lesson we will create a minimal Java application that uses the ClickHouse JDBC driver for querying a ClickHouse database.

Let's get started!


:::note Prerequisites
You have access to a machine that has:
1. a Unix shell and internet access 
2. <a href="https://www.gnu.org/software/wget/" target="_blank">wget</a> installed
3. a current version of **Java** (e.g. <a href="https://openjdk.java.net" target="_blank">OpenJDK</a> Version >= 17) installed
4. a current version of **ClickHouse** <a href="https://clickhouse.com/docs/en/getting-started/install/" target="_blank">installed</a> and running
:::


Let's start by connecting to a Unix shell on your machine where Java is installed and create a project directory for our minimal Java application (feel free to name the folder anything you like and put it anywhere you like):
 ```bash
 mkdir ~/hello-clickhouse-java-app
 ```

Now we download the <a href="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/" target="_blank">current version</a> of the ClickHouse JDBC driver into a subfolder of the project directory:
 ```bash
 cd ~/hello-clickhouse-java-app
 mkdir lib
 wget -P lib https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.3.2-patch7/clickhouse-jdbc-0.3.2-patch7-shaded.jar
 ```
   
   
Next we create a file for the Java main class of our minimal Java application in a subdirectory structure:
 ```bash
 cd ~/hello-clickhouse-java-app
 mkdir -p src/main/java/helloclickhouse
 touch src/main/java/helloclickhouse/HelloClickHouse.java
 ```
 You can now copy and paste the following Java code into the file `~/hello-clickhouse-java-app/src/main/java/helloclickhouse/HelloClickHouse.java`:
 ```java
 import com.clickhouse.jdbc.*;
 import java.sql.*;
 import java.util.*;
 
 public class HelloClickHouse {
     public static void main(String[] args) throws Exception {

         String url = "jdbc:ch://<host>:<port>";
         Properties properties = new Properties();
         // properties.setProperty("ssl", "true");
         // properties.setProperty("sslmode", "NONE"); // NONE to trust all servers; STRICT for trusted only
          
         ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
         try (Connection connection = dataSource.getConnection(<username>, <password>);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from system.tables limit 10")) {
             ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
             int columns = resultSetMetaData.getColumnCount();
             while (resultSet.next()) {
                 for (int c = 1; c <= columns; c++) {
                     System.out.print(resultSetMetaData.getColumnName(c) + ":" + resultSet.getString(c) + (c < columns ? ", " : "\n"));
                 }
             }
         }
     }
 }
 ```

:::note
in the Java class file above
   
   - in the first code line inside the main method you need to replace `<host>`, and `<port>` with values matching your running ClickHouse instance, e.g. `"jdbc:ch://localhost:8123"`
   - you also need to replace `<username>` and `<password>` with your ClickHouse instance credentials, if you don't use a password, you can replace `<password>` with `null`
:::


   
That was all! Now we are ready to start our minimal Java application from the Unix shell:
 ```bash
 cd ~/hello-clickhouse-java-app
 java -classpath lib/clickhouse-jdbc-0.3.2-patch7-shaded.jar  src/main/java/helloclickhouse/HelloClickHouse.java
 ```
   