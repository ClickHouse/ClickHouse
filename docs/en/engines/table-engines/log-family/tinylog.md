---
toc_priority: 34
toc_title: TinyLog
---

# TinyLog {#tinylog}

The engine belongs to the log engine family. See [Log Engine Family](../../../engines/table-engines/log-family/index.md) for common properties of log engines and their differences.

This table engine is typically used with the write-once method: write data one time, then read it as many times as necessary. For example, you can use `TinyLog`-type tables for intermediary data that is processed in small batches. Note that storing data in a large number of small tables is inefficient.

Queries are executed in a single stream. In other words, this engine is intended for relatively small tables (up to about 1,000,000 rows). It makes sense to use this table engine if you have many small tables, since it’s simpler than the [Log](../../../engines/table-engines/log-family/log.md) engine (fewer files need to be opened).

[Original article](https://clickhouse.com/docs/en/operations/table_engines/tinylog/) <!--hide-->
