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

The most universal and functional table engines for high-load tasks. The property shared by these engines is quick data insertion with subsequent background data processing. `MergeTree` family engines support data replication (with [Replicated*](mergetree-family/replication.md#replication) versions of engines), partitioning, and other features not supported in other engines.

Engines in the family:

-   [MergeTree](mergetree-family/mergetree.md#mergetree)
-   [ReplacingMergeTree](mergetree-family/replacingmergetree.md#replacingmergetree)
-   [SummingMergeTree](mergetree-family/summingmergetree.md#summingmergetree)
-   [AggregatingMergeTree](mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [CollapsingMergeTree](mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [GraphiteMergeTree](mergetree-family/graphitemergetree.md#graphitemergetree)

### Log {#log}

Lightweight [engines](log-family/index.md) with minimum functionality. They’re the most effective when you need to quickly write many small tables (up to approximately 1 million rows) and read them later as a whole.

Engines in the family:

-   [TinyLog](log-family/tinylog.md#tinylog)
-   [StripeLog](log-family/stripelog.md#stripelog)
-   [Log](log-family/log.md#log)

### Integration Engines {#integration-engines}

Engines for communicating with other data storage and processing systems.

Engines in the family:

-   [Kafka](integrations/kafka.md#kafka)
-   [MySQL](integrations/mysql.md#mysql)
-   [ODBC](integrations/odbc.md#table-engine-odbc)
-   [JDBC](integrations/jdbc.md#table-engine-jdbc)
-   [HDFS](integrations/hdfs.md#hdfs)

### Special Engines {#special-engines}

Engines in the family:

-   [Distributed](special/distributed.md#distributed)
-   [MaterializedView](special/materializedview.md#materializedview)
-   [Dictionary](special/dictionary.md#dictionary)
-   [Merge](special/merge.md#merge
-   [File](special/file.md#file)
-   [Null](special/null.md#null)
-   [Set](special/set.md#set)
-   [Join](special/join.md#join)
-   [URL](special/url.md#table_engines-url)
-   [View](special/view.md#table_engines-view)
-   [Memory](special/memory.md#memory)
-   [Buffer](special/buffer.md#buffer)

## Virtual Columns {#table_engines-virtual_columns}

Virtual column is an integral table engine attribute that is defined in the engine source code.

You shouldn’t specify virtual columns in the `CREATE TABLE` query and you can’t see them in `SHOW CREATE TABLE` and `DESCRIBE TABLE` query results. Virtual columns are also read-only, so you can’t insert data into virtual columns.

To select data from a virtual column, you must specify its name in the `SELECT` query. `SELECT *` doesn’t return values from virtual columns.

If you create a table with a column that has the same name as one of the table virtual columns, the virtual column becomes inaccessible. We don’t recommend doing this. To help avoid conflicts, virtual column names are usually prefixed with an underscore.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
