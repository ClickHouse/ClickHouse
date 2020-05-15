---
toc_priority: 33
toc_title: Log
---

# Log {#log}

Engine belongs to the family of log engines. See the common properties of log engines and their differences in the [Log Engine Family](log-family.md) article.

Log differs from [TinyLog](tinylog.md) in that a small file of “marks” resides with the column files. These marks are written on every data block and contain offsets that indicate where to start reading the file in order to skip the specified number of rows. This makes it possible to read table data in multiple threads.
For concurrent data access, the read operations can be performed simultaneously, while write operations block reads and each other.
The Log engine does not support indexes. Similarly, if writing to a table failed, the table is broken, and reading from it returns an error. The Log engine is appropriate for temporary data, write-once tables, and for testing or demonstration purposes.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/log/) <!--hide-->
