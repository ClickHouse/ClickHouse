---
toc_folder_title: Integrations
toc_priority: 30
---

# Table Engines for Integrations

ClickHouse provides various means for integrating with external systems, including table engines. Like with all other table engines, the configuration is done using `CREATE TABLE` or `ALTER TABLE` queries. Then from a user perspective, the configured integration looks like a normal table, but queries to it are proxied to the external system. This transparent querying is one of the key advantages of this approach over alternative integration methods, like external dictionaries or table functions, which require to use custom query methods on each use.

List of supported integrations:

-   [ODBC](odbc.md)
-   [JDBC](jdbc.md)
-   [MySQL](mysql.md)
-   [HDFS](hdfs.md)
-   [Kafka](kafka.md)
