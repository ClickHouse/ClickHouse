# Table engines {#table_engines}

The table engine (type of table) determines:

- How and where data is stored, where to write it to, and from where to read it.
- Which queries are supported, and how.
- Concurrent data access.
- Use of indexes, if present.
- Whether multithreaded request execution is possible.
- Data replication parameters.

## Engine Families

### *MergeTree

The most universal and functional table engines for high-load tasks. The common property of these engines is quick data insertion with subsequent data processing in the background. The `*MergeTree` engines support data replication (with [Replicated*](replication.md) versions of engines), partitioning and other features not supported in other engines.

Engines of the family:

- [MergTree](mergetree.md)
- [ReplacingMergeTree](replacingmergetree.md)
- [SummingMergeTree](summingmergetree.md)
- [AggregatingMergeTree](aggregatingmergetree.md)
- [CollapsingMergeTree](collapsingmergetree.md)
- [VersionedCollapsingMergeTree](versionedcollapsingmergetree.md)
- [GraphiteMergeTree](graphitemergetree.md)

### *Log

Lightweight [engines](log_family.md) with minimum functionality. They are the most effective in scenarios when you need to quickly write many small tables (up to about 1 million rows) and read them later as a whole.

Engines of the family:

- [TinyLog](tinylog.md)
- [StripeLog](stripelog.md)
- [Log](log.md)

### Intergation engines

Engines for communicating with other data storage and processing systems.

Engines of the family:

- [Kafka](kafka.md)
- [MySQL](mysql.md)
- [ODBC](odbc.md)
- [JDBC](jdbc.md)

### Special engines

Engines solving special tasks.

Engines of the family:

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

Virtual column is an integral attribute of a table engine that is defined in the source code of the engine.

You should not specify virtual columns in the `CREATE TABLE` query, and you cannot see them in the results of `SHOW CREATE TABLE` and `DESCRIBE TABLE` queries. Also, virtual columns are read-only, so you can't insert data into virtual columns.

To select data from a virtual column, you must specify its name in the `SELECT` query. The `SELECT *` doesn't return values from virtual columns.

If you create a table with a column that has the same name as one of the table virtual columns, the virtual column becomes inaccessible. Doing so is not recommended. To help avoiding conflicts virtual column names are usually prefixed with an underscore.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/) <!--hide-->
