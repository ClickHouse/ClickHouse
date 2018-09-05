<a name="table_engines"></a>

# Table engines

The table engine (type of table) determines:

- How and where data is stored, where to write it to, and where to read it from.
- Which queries are supported, and how.
- Concurrent data access.
- Use of indexes, if present.
- Whether multithreaded request execution is possible.
- Data replication parameters.

When reading, the engine is only required to output the requested columns, but in some cases the engine can partially process data when responding to the request.

For most serious tasks, you should use engines from the `MergeTree` family.
