---
toc_folder_title: Table Engines
toc_priority: 26
toc_title: Introduction
---

# Table Engines {#table_engines}

The table engine (type of table) determines:

-   How and where data is stored, where to write it to, and where to read it from.
-   Which queries are supported, and how.
-   Concurrent data access.
-   Use of indexes, if present.
-   Whether multithreaded request execution is possible.
-   Data replication parameters.

## Engine Families {#engine-families}

### MergeTree {#mergetree}

The most universal and functional table engines for high-load tasks. The property shared by these engines is quick data insertion with subsequent background data processing. `MergeTree` family engines support data replication (with [Replicated\*](mergetree-family/replication.md) versions of engines), partitioning, and other features not supported in other engines.

Engines in the family:

-   [MergeTree](mergetree-family/mergetree.md)
-   [ReplacingMergeTree](mergetree-family/replacingmergetree.md)
-   [SummingMergeTree](mergetree-family/summingmergetree.md)
-   [AggregatingMergeTree](mergetree-family/aggregatingmergetree.md)
-   [CollapsingMergeTree](mergetree-family/collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md)
-   [GraphiteMergeTree](mergetree-family/graphitemergetree.md)

### Log {#log}

Lightweight [engines](log-family/index.md) with minimum functionality. They’re the most effective when you need to quickly write many small tables (up to approximately 1 million rows) and read them later as a whole.

Engines in the family:

-   [TinyLog](log-family/tinylog.md)
-   [StripeLog](log-family/stripelog.md)
-   [Log](log-family/log.md)

### Integration Engines {#integration-engines}

Engines for communicating with other data storage and processing systems.

Engines in the family:

-   [Kafka](integrations/kafka.md)
-   [MySQL](integrations/mysql.md)
-   [ODBC](integrations/odbc.md)
-   [JDBC](integrations/jdbc.md)
-   [HDFS](integrations/hdfs.md)

### Special Engines {#special-engines}

Engines in the family:

-   [Distributed](special/distributed.md)
-   [MaterializedView](special/materializedview.md)
-   [Dictionary](special/dictionary.md)
-   [Merge](special/merge.md)
-   [File](special/file.md)
-   [Null](special/null.md)
-   [Set](special/set.md)
-   [Join](special/join.md)
-   [URL](special/url.md)
-   [View](special/view.md)
-   [Memory](special/memory.md)
-   [Buffer](special/buffer.md)

## Virtual Columns {#table_engines-virtual-columns}

Virtual column is an integral table engine attribute that is defined in the engine source code.

You shouldn’t specify virtual columns in the `CREATE TABLE` query and you can’t see them in `SHOW CREATE TABLE` and `DESCRIBE TABLE` query results. Virtual columns are also read-only, so you can’t insert data into virtual columns.

To select data from a virtual column, you must specify its name in the `SELECT` query. `SELECT *` doesn’t return values from virtual columns.

If you create a table with a column that has the same name as one of the table virtual columns, the virtual column becomes inaccessible. We don’t recommend doing this. To help avoid conflicts, virtual column names are usually prefixed with an underscore.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
