# TinyLog

Engine belongs to the family of log engines. See [Log Engine Family](log_family.md) for common properties of log engines and for their differences.

The typical way using this table engine is write-once method: firstly write the data one time, then read it as many times as needed. For example, you can use `TinyLog`-type tables for intermediary data that is processed in small batches.

Queries are executed in a single stream. In other words, this engine is intended for relatively small tables (recommended up to about 1,000,000 rows). It makes sense to use this table engine if you have many small tables, since it is simpler than the [Log](log.md) engine (fewer files need to be opened).

The situation when you have a large number of small tables guarantees poor productivity, but may already be used when working with another DBMS, and you may find it easier to switch to using `TinyLog`-type tables.


[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/tinylog/) <!--hide-->
