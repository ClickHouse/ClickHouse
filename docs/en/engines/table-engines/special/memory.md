---
toc_priority: 44
toc_title: Memory
---

# Memory Table Engine {#memory}

The Memory engine stores data in RAM, in uncompressed form. Data is stored in exactly the same form as it is received when read. In other words, reading from this table is completely free.
Concurrent data access is synchronized. Locks are short: read and write operations do not block each other.
Indexes are not supported. Reading is parallelized.

Maximal productivity (over 10 GB/sec) is reached on simple queries, because there is no reading from the disk, decompressing, or deserializing data. (We should note that in many cases, the productivity of the MergeTree engine is almost as high.)
When restarting a server, data disappears from the table and the table becomes empty.
Normally, using this table engine is not justified. However, it can be used for tests, and for tasks where maximum speed is required on a relatively small number of rows (up to approximately 100,000,000).

The Memory engine is used by the system for temporary tables with external query data (see the section “External data for processing a query”), and for implementing `GLOBAL IN` (see the section “IN operators”).

[Original article](https://clickhouse.com/docs/en/operations/table_engines/memory/) <!--hide-->
