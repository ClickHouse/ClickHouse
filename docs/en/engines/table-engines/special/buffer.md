---
toc_priority: 45
toc_title: Buffer
---

# Buffer Table Engine {#buffer}

Buffers the data to write in RAM, periodically flushing it to another table. During the read operation, data is read from the buffer and the other table simultaneously.

``` sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
```

Engine parameters:

-   `database` – Database name. Instead of the database name, you can use a constant expression that returns a string.
-   `table` – Table to flush data to.
-   `num_layers` – Parallelism layer. Physically, the table will be represented as `num_layers` of independent buffers. Recommended value: 16.
-   `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, and `max_bytes` – Conditions for flushing data from the buffer.

Optional engine parameters:

-   `flush_time`, `flush_rows`, `flush_bytes` – Conditions for flushing data from the buffer, that will happen only in background (omitted or zero means no `flush*` parameters).

Data is flushed from the buffer and written to the destination table if all the `min*` conditions or at least one `max*` condition are met.

Also, if at least one `flush*` condition are met flush initiated in background, this is different from `max*`, since `flush*` allows you to configure background flushes separately to avoid adding latency for `INSERT` (into `Buffer`) queries.

-   `min_time`, `max_time`, `flush_time` – Condition for the time in seconds from the moment of the first write to the buffer.
-   `min_rows`, `max_rows`, `flush_rows` – Condition for the number of rows in the buffer.
-   `min_bytes`, `max_bytes`, `flush_bytes` – Condition for the number of bytes in the buffer.

During the write operation, data is inserted to a `num_layers` number of random buffers. Or, if the data part to insert is large enough (greater than `max_rows` or `max_bytes`), it is written directly to the destination table, omitting the buffer.

The conditions for flushing the data are calculated separately for each of the `num_layers` buffers. For example, if `num_layers = 16` and `max_bytes = 100000000`, the maximum RAM consumption is 1.6 GB.

Example:

``` sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
```

Creating a `merge.hits_buffer` table with the same structure as `merge.hits` and using the Buffer engine. When writing to this table, data is buffered in RAM and later written to the ‘merge.hits’ table. 16 buffers are created. The data in each of them is flushed if either 100 seconds have passed, or one million rows have been written, or 100 MB of data have been written; or if simultaneously 10 seconds have passed and 10,000 rows and 10 MB of data have been written. For example, if just one row has been written, after 100 seconds it will be flushed, no matter what. But if many rows have been written, the data will be flushed sooner.

When the server is stopped, with `DROP TABLE` or `DETACH TABLE`, buffer data is also flushed to the destination table.

You can set empty strings in single quotation marks for the database and table name. This indicates the absence of a destination table. In this case, when the data flush conditions are reached, the buffer is simply cleared. This may be useful for keeping a window of data in memory.

When reading from a Buffer table, data is processed both from the buffer and from the destination table (if there is one).
Note that the Buffer tables does not support an index. In other words, data in the buffer is fully scanned, which might be slow for large buffers. (For data in a subordinate table, the index that it supports will be used.)

If the set of columns in the Buffer table does not match the set of columns in a subordinate table, a subset of columns that exist in both tables is inserted.

If the types do not match for one of the columns in the Buffer table and a subordinate table, an error message is entered in the server log, and the buffer is cleared.
The same thing happens if the subordinate table does not exist when the buffer is flushed.

If you need to run ALTER for a subordinate table, and the Buffer table, we recommend first deleting the Buffer table, running ALTER for the subordinate table, then creating the Buffer table again.

If the server is restarted abnormally, the data in the buffer is lost.

`FINAL` and `SAMPLE` do not work correctly for Buffer tables. These conditions are passed to the destination table, but are not used for processing data in the buffer. If these features are required we recommend only using the Buffer table for writing, while reading from the destination table.

When adding data to a Buffer, one of the buffers is locked. This causes delays if a read operation is simultaneously being performed from the table.

Data that is inserted to a Buffer table may end up in the subordinate table in a different order and in different blocks. Because of this, a Buffer table is difficult to use for writing to a CollapsingMergeTree correctly. To avoid problems, you can set `num_layers` to 1.

If the destination table is replicated, some expected characteristics of replicated tables are lost when writing to a Buffer table. The random changes to the order of rows and sizes of data parts cause data deduplication to quit working, which means it is not possible to have a reliable ‘exactly once’ write to replicated tables.

Due to these disadvantages, we can only recommend using a Buffer table in rare cases.

A Buffer table is used when too many INSERTs are received from a large number of servers over a unit of time and data can’t be buffered before insertion, which means the INSERTs can’t run fast enough.

Note that it does not make sense to insert data one row at a time, even for Buffer tables. This will only produce a speed of a few thousand rows per second, while inserting larger blocks of data can produce over a million rows per second (see the section “Performance”).

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/buffer/) <!--hide-->
