# Table engines {#table_engines}

The table engine (type of table) determines:

- How and where data is stored, where to write it to, and where to read it from.
- Which queries are supported, and how.
- Concurrent data access.
- Use of indexes, if present.
- Whether multithreaded request execution is possible.
- Data replication parameters.

## Engine Families

### MergeTree

The most universal and functional table engines for high-load tasks. The property shared by these engines is quick data insertion with subsequent background data processing. `MergeTree` family engines support data replication (with [Replicated*](replication.md) versions of engines), partitioning, and other features not supported in other engines.

Engines in the family:

- [MergeTree](mergetree.md)
- [ReplacingMergeTree](replacingmergetree.md)
- [SummingMergeTree](summingmergetree.md)
- [AggregatingMergeTree](aggregatingmergetree.md)
- [CollapsingMergeTree](collapsingmergetree.md)
- [VersionedCollapsingMergeTree](versionedcollapsingmergetree.md)
- [GraphiteMergeTree](graphitemergetree.md)

### Log

Lightweight [engines](log_family.md) with minimum functionality. They're the most effective when you need to quickly write many small tables (up to approximately 1 million rows) and read them later as a whole.

Engines in the family:

- [TinyLog](tinylog.md)
- [StripeLog](stripelog.md)
- [Log](log.md)

### Intergation engines

Engines for communicating with other data storage and processing systems.

Engines in the family:

- [Kafka](kafka.md)
- [MySQL](mysql.md)
- [ODBC](odbc.md)
- [JDBC](jdbc.md)
- [HDFS](hdfs.md)

### Special engines

Engines in the family:

- [Distributed](distributed.md)
- [MaterializedView](materializedview.md)
- [Dictionary](dictionary.md)
- [Merge](merge.md)
- [File](file.md)
- [Null](null.md)
- [Set](set.md)
- [Join](join.md)
- [URL](url.md)
- [View](view.md)
- [Memory](memory.md)
- [Buffer](buffer.md)

## Virtual columns {#table_engines-virtual_columns}

Virtual column is an integral table engine attribute that is defined in the engine source code.

You shouldn't specify virtual columns in the `CREATE TABLE` query and you can't see them in `SHOW CREATE TABLE` and `DESCRIBE TABLE` query results. Virtual columns are also read-only, so you can't insert data into virtual columns.

To select data from a virtual column, you must specify its name in the `SELECT` query. `SELECT *` doesn't return values from virtual columns.

If you create a table with a column that has the same name as one of the table virtual columns, the virtual column becomes inaccessible. We don't recommend doing this. To help avoid conflicts, virtual column names are usually prefixed with an underscore.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/) <!--hide-->
