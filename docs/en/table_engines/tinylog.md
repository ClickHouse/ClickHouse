# TinyLog

The simplest table engine, which stores data on a disk.
Each column is stored in a separate compressed file.
When writing, data is appended to the end of files.

Concurrent data access is not restricted in any way:

- If you are simultaneously reading from a table and writing to it in a different query, the read operation will complete with an error.
- If you are writing to a table in multiple queries simultaneously, the data will be broken.

The typical way to use this table is write-once: first just write the data one time, then read it as many times as needed.
Queries are executed in a single stream. In other words, this engine is intended for relatively small tables (recommended up to 1,000,000 rows).
It makes sense to use this table engine if you have many small tables, since it is simpler than the Log engine (fewer files need to be opened).
The situation when you have a large number of small tables guarantees poor productivity, but may already be used when working with another DBMS, and you may find it easier to switch to using TinyLog types of tables.
**Indexes are not supported.**

In Yandex.Metrica, TinyLog tables are used for intermediary data that is processed in small batches.

