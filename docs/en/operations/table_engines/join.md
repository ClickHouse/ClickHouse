# Join

Prepared data structure for using in [JOIN](../../query_language/select.md#select-join) operations.

## Creating a Table

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

See the detailed description of the [CREATE TABLE](../../query_language/create.md#create-table-query) query.

**Engine Parameters**

- `join_strictness` – [JOIN strictness](../../query_language/select.md#select-join-strictness).
- `join_type` – [JOIN type](../../query_language/select.md#select-join-types).
- `k1[, k2, ...]` – Key columns from the `USING` clause that the `JOIN` operation is made with.

Enter `join_strictness` and `join_type` parameters without quotes, for example, `Join(ANY, LEFT, col1)`. They must match the `JOIN` operation that the table will be used for. If the parameters don't match, ClickHouse doesn't throw an exception and may return incorrect data.

## Table Usage

### Example

Creating the left-side table:

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog
```
```sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13)
```

Creating the right-side `Join` table:

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id)
```
```sql
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

As an alternative, you can retrieve data from the `Join` table, specifying the join key value:

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```
```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### Selecting and Inserting Data

You can use `INSERT` queries to add data to the `Join`-engine tables. If the table was created with the `ANY` strictness, data for duplicate keys are ignored. With the `ALL` strictness, all rows are added.

You cannot perform a `SELECT` query directly from the table. Instead, use one of the following methods:

- Place the table to the right side in a `JOIN` clause.
- Call the [joinGet](../../query_language/functions/other_functions.md#other_functions-joinget) function, which lets you extract data from the table the same way as from a dictionary.

### Limitations and Settings

When creating a table, the following settings are applied:

- [join_use_nulls](../settings/settings.md#settings-join_use_nulls)
- [max_rows_in_join](../settings/query_complexity.md#settings-max_rows_in_join)
- [max_bytes_in_join](../settings/query_complexity.md#settings-max_bytes_in_join)
- [join_overflow_mode](../settings/query_complexity.md#settings-join_overflow_mode)
- [join_any_take_last_row](../settings/settings.md#settings-join_any_take_last_row)

The `Join`-engine tables can't be used in `GLOBAL JOIN` operations.

## Data Storage

`Join` table data is always located in the RAM. When inserting rows into a table, ClickHouse writes data blocks to the directory on the disk so that they can be restored when the server restarts.

If the server restarts incorrectly, the data block on the disk might get lost or damaged. In this case, you may need to manually delete the file with damaged data.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/join/) <!--hide-->
