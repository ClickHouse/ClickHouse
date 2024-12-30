---
slug: /en/engines/table-engines/special/memory
sidebar_position: 110
sidebar_label:  Memory
---

# Memory Table Engine

:::note
When using the Memory table engine on ClickHouse Cloud, data is not replicated across all nodes (by design). To guarantee that all queries are routed to the same node and that the Memory table engine works as expected, you can do one of the following:
- Execute all operations in the same session
- Use a client that uses TCP or the native interface (which enables support for sticky connections) such as [clickhouse-client](/en/interfaces/cli)
:::

The Memory engine stores data in RAM, in uncompressed form. Data is stored in exactly the same form as it is received when read. In other words, reading from this table is completely free.
Concurrent data access is synchronized. Locks are short: read and write operations do not block each other.
Indexes are not supported. Reading is parallelized.

Maximal productivity (over 10 GB/sec) is reached on simple queries, because there is no reading from the disk, decompressing, or deserializing data. (We should note that in many cases, the productivity of the MergeTree engine is almost as high.)
When restarting a server, data disappears from the table and the table becomes empty.
Normally, using this table engine is not justified. However, it can be used for tests, and for tasks where maximum speed is required on a relatively small number of rows (up to approximately 100,000,000).

The Memory engine is used by the system for temporary tables with external query data (see the section “External data for processing a query”), and for implementing `GLOBAL IN` (see the section “IN operators”).

Upper and lower bounds can be specified to limit Memory engine table size, effectively allowing it to act as a circular buffer (see [Engine Parameters](#engine-parameters)).

## Engine Parameters {#engine-parameters}

- `min_bytes_to_keep` — Minimum bytes to keep when memory table is size-capped.
  - Default value: `0`
  - Requires `max_bytes_to_keep`
- `max_bytes_to_keep` — Maximum bytes to keep within memory table where oldest rows are deleted on each insertion (i.e circular buffer). Max bytes can exceed the stated limit if the oldest batch of rows to remove falls under the `min_bytes_to_keep` limit when adding a large block.
  - Default value: `0`
- `min_rows_to_keep` — Minimum rows to keep when memory table is size-capped.
  - Default value: `0`
  - Requires `max_rows_to_keep`
- `max_rows_to_keep` — Maximum rows to keep within memory table where oldest rows are deleted on each insertion (i.e circular buffer). Max rows can exceed the stated limit if the oldest batch of rows to remove falls under the `min_rows_to_keep` limit when adding a large block.
  - Default value: `0`
- `compress` - Whether to compress data in memory.
  - Default value: `false`

## Usage {#usage}


**Initialize settings**
``` sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Modify settings**
```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Note:** Both `bytes` and `rows` capping parameters can be set at the same time, however, the lower bounds of `max` and `min` will be adhered to.

## Examples {#examples}
``` sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();
```

``` text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

also, for rows:

``` sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();
```

``` text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

