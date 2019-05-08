# Join

Always located in RAM, prepared data structure for using in `JOIN` operations.

## Creating a table

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

Set the parameters `join_strictness` and `join_type` without quotes. They must match the `JOIN` operation that the table will be used for. For more information about `JOIN` type and strictness, see the [JOIN Clause](../../query_language/select.md#select-join) section.


## Table Usage

The table can't be used in `GLOBAL` `JOIN` operations.

When creating a table, the following settings are applied:

- [join_use_nulls](../settings/settings.md#settings-join_use_nulls)
- [max_rows_in_join](../settings/settings.md)
- [max_bytes_in_join](../settings/settings.md)
- [join_overflow_mode](../settings/settings.md)

## Selecting and Inserting data

You can use `INSERT` to add data to the table. For the `ANY` strictness, data for duplicated keys are ignored. For the `ALL` strictness, data are counted.

You cannot perform the `SELECT` query directly from the table. Use the table from the right side in `JOIN` clause.

## Data Storage

Data for the `Join`-type tables is always located in RAM. When inserting rows into the table, ClickHouse writes the data blocks to the directory of tables on the disk. When starting the server, this data is loaded to RAM.

At the abnormal server restart, the block of data on the disk might be lost or damaged. In this case, you may need to manually delete the file with damaged data.


[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/join/) <!--hide-->
