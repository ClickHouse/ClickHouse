# Join

Prepared data structure for using in [JOIN](../../query_language/select.md#select-join) operations.

## Creating a Table

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

See the detailed description of [CREATE TABLE](../../query_language/create.md#create-table-query) query.

**Engine Parameters**

- `join_strictness` – `JOIN` strictness.
- `join_type` – `JOIN` type.
- `k1[, k2, ...]` – Key columns from the `USING` clause that the `JOIN` operation is made with.

Set the parameters `join_strictness` and `join_type` without quotes, for example, `Join(ANY, LEFT, col1)`. They must match the `JOIN` operation that the table will be used for. For more information about `JOIN` type and strictness, see the `JOIN` clause description.

## Table Usage

### Example

Creating the right-side table:

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
INSERT INTO id_val VALUES (1,11)(2,12)(3,13);
```

Creating the left-side `Join` table:

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23)
```

Joining the tables:

```sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id) SETTINGS join_use_nulls = 1
```
```text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │            ᴺᵁᴸᴸ │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

Retrieving the data from the `Join` table, specifying the join key value:

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```
```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### Selecting and Inserting Data

You can use `INSERT` to add data to the table. For the `ANY` strictness, data for duplicated keys are ignored. For the `ALL` strictness, all rows are kept.

You cannot perform the `SELECT` query directly from the table. Use one of the following ways:

- Place the table at the right side in a `JOIN` clause.
- Call the [joinGet](../../query_language/functions/other_functions.md#other_functions-joinget) function, which allows to extract data from the table as from a dictionary.

### Limitations and Settings

When creating a table, the following settings are applied:

- [join_use_nulls](../settings/settings.md#settings-join_use_nulls)
- [max_rows_in_join](../settings/query_complexity.md#settings-max_rows_in_join)
- [max_bytes_in_join](../settings/query_complexity.md#settings-max_bytes_in_join)
- [join_overflow_mode](../settings/query_complexity.md#settings-join_overflow_mode)
- [join_any_take_last_row](../settings/settings.md#settings-join_any_take_last_row)

The table can't be used in `GLOBAL JOIN` operations.

## Data Storage

Data for the `Join` tables is always located in RAM. When inserting rows into the table, ClickHouse writes the data blocks to the directory on disk to be able to restore them on server restart.

At the abnormal server restart, the block of data on the disk might be lost or damaged. In this case, you may need to manually delete the file with damaged data.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/join/) <!--hide-->
