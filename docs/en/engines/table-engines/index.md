---
toc_folder_title: Table Engines
toc_priority: 26
toc_title: Introduction
---

# Table Engines

The table engine (type of table) determines:

-   How and where data is stored, where to write it to, and where to read it from.
-   Which queries are supported, and how.
-   Concurrent data access.
-   Use of indexes, if present.
-   Whether multithread request execution is possible.
-   Data replication parameters.

## Engine Families {#engine-families}

### MergeTree {#mergetree}

The most universal and functional table engines for high-load tasks. The property shared by these engines is quick data insertion with subsequent background data processing. `MergeTree` family engines support data replication (with [Replicated\*](../../engines/table-engines/mergetree-family/replication.md#table_engines-replication) versions of engines), partitioning, secondary data-skipping indexes, and other features not supported in other engines.

Engines in the family:

-   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#mergetree)
-   [ReplacingMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md#replacingmergetree)
-   [SummingMergeTree](../../engines/table-engines/mergetree-family/summingmergetree.md#summingmergetree)
-   [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [CollapsingMergeTree](../../engines/table-engines/mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [VersionedCollapsingMergeTree](../../engines/table-engines/mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md#graphitemergetree)

### Log {#log}

Lightweight [engines](../../engines/table-engines/log-family/index.md) with minimum functionality. They’re the most effective when you need to quickly write many small tables (up to approximately 1 million rows) and read them later as a whole.

Engines in the family:

-   [TinyLog](../../engines/table-engines/log-family/tinylog.md#tinylog)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md#stripelog)
-   [Log](../../engines/table-engines/log-family/log.md#log)

### Integration Engines {#integration-engines}

Engines for communicating with other data storage and processing systems.

Engines in the family:


-   [ODBC](../../engines/table-engines/integrations/odbc.md)
-   [JDBC](../../engines/table-engines/integrations/jdbc.md)
-   [MySQL](../../engines/table-engines/integrations/mysql.md)
-   [MongoDB](../../engines/table-engines/integrations/mongodb.md)
-   [HDFS](../../engines/table-engines/integrations/hdfs.md)
-   [S3](../../engines/table-engines/integrations/s3.md)
-   [Kafka](../../engines/table-engines/integrations/kafka.md)
-   [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md)
-   [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)
-   [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)

### Special Engines {#special-engines}

Engines in the family:

-   [Distributed](../../engines/table-engines/special/distributed.md#distributed)
-   [MaterializedView](../../engines/table-engines/special/materializedview.md#materializedview)
-   [Dictionary](../../engines/table-engines/special/dictionary.md#dictionary)
-   [Merge](../../engines/table-engines/special/merge.md#merge)
-   [File](../../engines/table-engines/special/file.md#file)
-   [Null](../../engines/table-engines/special/null.md#null)
-   [Set](../../engines/table-engines/special/set.md#set)
-   [Join](../../engines/table-engines/special/join.md#join)
-   [URL](../../engines/table-engines/special/url.md#table_engines-url)
-   [View](../../engines/table-engines/special/view.md#table_engines-view)
-   [Memory](../../engines/table-engines/special/memory.md#memory)
-   [Buffer](../../engines/table-engines/special/buffer.md#buffer)

## Virtual Columns {#table_engines-virtual_columns}

Virtual column is an integral table engine attribute that is defined in the engine source code.

You shouldn’t specify virtual columns in the `CREATE TABLE` query and you can’t see them in `SHOW CREATE TABLE` and `DESCRIBE TABLE` query results. Virtual columns are also read-only, so you can’t insert data into virtual columns.

To select data from a virtual column, you must specify its name in the `SELECT` query. `SELECT *` does not return values from virtual columns.

If you create a table with a column that has the same name as one of the table virtual columns, the virtual column becomes inaccessible. We do not recommend doing this. To help avoid conflicts, virtual column names are usually prefixed with an underscore.

[Original article](https://clickhouse.com/docs/en/engines/table-engines/) <!--hide-->
