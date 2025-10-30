---
description: 'Optional prepared data structure for usage in JOIN operations.'
sidebar_label: 'Join'
sidebar_position: 70
slug: /engines/table-engines/special/join
title: 'Join Table Engine'
---

# Join Table Engine

Optional prepared data structure for usage in [JOIN](/sql-reference/statements/select/join) operations.

:::note
This is not an article about the [JOIN clause](/sql-reference/statements/select/join) itself.
:::

:::note
In ClickHouse Cloud, if your service was created with a version earlier than 25.4, you will need to set the compatibility to at least 25.4 using  `SET compatibility=25.4`.
:::

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

See the detailed description of the [CREATE TABLE](/sql-reference/statements/create/table) query.

## Engine Parameters {#engine-parameters}

### join_strictness {#join_strictness}

`join_strictness` – [JOIN strictness](/sql-reference/statements/select/join#supported-types-of-join).

### join_type {#join_type}

`join_type` – [JOIN type](/sql-reference/statements/select/join#supported-types-of-join).

### Key columns {#key-columns}

`k1[, k2, ...]` – Key columns from the `USING` clause that the `JOIN` operation is made with.

Enter `join_strictness` and `join_type` parameters without quotes, for example, `Join(ANY, LEFT, col1)`. They must match the `JOIN` operation that the table will be used for. If the parameters do not match, ClickHouse does not throw an exception and may return incorrect data.

## Specifics and Recommendations {#specifics-and-recommendations}

### Data Storage {#data-storage}

`Join` table data is always located in the RAM. When inserting rows into a table, ClickHouse writes data blocks to the directory on the disk so that they can be restored when the server restarts.

If the server restarts incorrectly, the data block on the disk might get lost or damaged. In this case, you may need to manually delete the file with damaged data.

### Selecting and Inserting Data {#selecting-and-inserting-data}

You can use `INSERT` queries to add data to the `Join`-engine tables. If the table was created with the `ANY` strictness, data for duplicate keys are ignored. With the `ALL` strictness, all rows are added.

Main use-cases for `Join`-engine tables are following:

- Place the table to the right side in a `JOIN` clause.
- Call the [joinGet](/sql-reference/functions/other-functions.md/#joinget) function, which lets you extract data from the table the same way as from a dictionary.

### Deleting Data {#deleting-data}

`ALTER DELETE` queries for `Join`-engine tables are implemented as [mutations](/sql-reference/statements/alter/index.md#mutations). `DELETE` mutation reads filtered data and overwrites data of memory and disk.

### Limitations and Settings {#join-limitations-and-settings}

When creating a table, the following settings are applied:

#### join_use_nulls {#join_use_nulls}

[join_use_nulls](/operations/settings/settings.md/#join_use_nulls)

#### max_rows_in_join {#max_rows_in_join}

[max_rows_in_join](/operations/settings/settings#max_rows_in_join)

#### max_bytes_in_join {#max_bytes_in_join}

[max_bytes_in_join](/operations/settings/settings#max_bytes_in_join)

#### join_overflow_mode {#join_overflow_mode}

[join_overflow_mode](/operations/settings/settings#join_overflow_mode)

#### join_any_take_last_row {#join_any_take_last_row}

[join_any_take_last_row](/operations/settings/settings.md/#join_any_take_last_row)
#### join_use_nulls {#join_use_nulls-1}

#### persistent {#persistent}

Disables persistency for the Join and [Set](/engines/table-engines/special/set.md) table engines.

Reduces the I/O overhead. Suitable for scenarios that pursue performance and do not require persistence.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.

The `Join`-engine tables can't be used in `GLOBAL JOIN` operations.

The `Join`-engine allows to specify [join_use_nulls](/operations/settings/settings.md/#join_use_nulls) setting in the `CREATE TABLE` statement. [SELECT](/sql-reference/statements/select/index.md) query should have the same `join_use_nulls` value.

## Usage Examples {#example}

Creating the left-side table:

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
```

```sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13);
```

Creating the right-side `Join` table:

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
```

```sql
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23);
```

Joining the tables:

```sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id);
```

```text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │               0 │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

As an alternative, you can retrieve data from the `Join` table, specifying the join key value:

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1));
```

```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

Deleting a row from the `Join` table:

```sql
ALTER TABLE id_val_join DELETE WHERE id = 3;
```

```text
┌─id─┬─val─┐
│  1 │  21 │
└────┴─────┘
```
