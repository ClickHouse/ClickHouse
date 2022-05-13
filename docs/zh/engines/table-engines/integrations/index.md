---
sidebar_label: 集成的表引擎
sidebar_position: 30
---

# 集成的表引擎 {#table-engines-for-integrations}

ClickHouse 提供了多种方式来与外部系统集成，包括表引擎。像所有其他的表引擎一样，使用`CREATE TABLE`或`ALTER TABLE`查询语句来完成配置。然后从用户的角度来看，配置的集成看起来像查询一个正常的表，但对它的查询是代理给外部系统的。这种透明的查询是这种方法相对于其他集成方法的主要优势之一，比如外部字典或表函数，它们需要在每次使用时使用自定义查询方法。

以下是支持的集成方式:

-   [ODBC](../../../engines/table-engines/integrations/odbc.md)
-   [JDBC](../../../engines/table-engines/integrations/jdbc.md)
-   [MySQL](../../../engines/table-engines/integrations/mysql.md)
-   [MongoDB](../../../engines/table-engines/integrations/mongodb.md)
-   [HDFS](../../../engines/table-engines/integrations/hdfs.md)
-   [S3](../../../engines/table-engines/integrations/s3.md)
-   [Kafka](../../../engines/table-engines/integrations/kafka.md)
-   [EmbeddedRocksDB](../../../engines/table-engines/integrations/embedded-rocksdb.md)
-   [RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md)
-   [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)
-   [SQLite](../../../engines/table-engines/integrations/sqlite.md)
-   [Hive](../../../engines/table-engines/integrations/hive.md)
