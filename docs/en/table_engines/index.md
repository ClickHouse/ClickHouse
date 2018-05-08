# Table engines

The table engine (type of table) determines:

- How and where data is stored: where to write it to, and where to read it from.
- Which queries are supported, and how.
- Concurrent data access.
- Use of indexes, if present.
- Whether multithreaded request execution is possible.
- Data replication.

When reading data, the engine is only required to extract the necessary set of columns. However, in some cases, the query may be partially processed inside the table engine.

Note that for most serious tasks, you should use engines from the `MergeTree` family.
