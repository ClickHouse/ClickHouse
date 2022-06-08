---
toc_priority: 33
toc_title: Log
---

# Log

The engine belongs to the family of `Log` engines. See the common properties of `Log` engines and their differences in the [Log Engine Family](../../../engines/table-engines/log-family/index.md) article.

`Log` differs from [TinyLog](../../../engines/table-engines/log-family/tinylog.md) in that a small file of "marks" resides with the column files. These marks are written on every data block and contain offsets that indicate where to start reading the file in order to skip the specified number of rows. This makes it possible to read table data in multiple threads.
For concurrent data access, the read operations can be performed simultaneously, while write operations block reads and each other.
The `Log` engine does not support indexes. Similarly, if writing to a table failed, the table is broken, and reading from it returns an error. The `Log` engine is appropriate for temporary data, write-once tables, and for testing or demonstration purposes.

[Original article](https://clickhouse.com/docs/en/engines/table-engines/log-family/log/) <!--hide-->

