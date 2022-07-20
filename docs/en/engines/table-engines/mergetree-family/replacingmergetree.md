---
sidebar_position: 40
sidebar_label:  ReplacingMergeTree
---

# ReplacingMergeTree

The engine differs from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree) in that it removes duplicate entries with the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md) value (`ORDER BY` table section, not `PRIMARY KEY`).

Data deduplication occurs only during a merge. Merging occurs in the background at an unknown time, so you can’t plan for it. Some of the data may remain unprocessed. Although you can run an unscheduled merge using the `OPTIMIZE` query, do not count on using it, because the `OPTIMIZE` query will read and write a large amount of data.

Thus, `ReplacingMergeTree` is suitable for clearing out duplicate data in the background in order to save space, but it does not guarantee the absence of duplicates.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [statement description](../../../sql-reference/statements/create/table.md).

:::warning
Uniqueness of rows is determined by the `ORDER BY` table section, not `PRIMARY KEY`.
:::

## ReplacingMergeTree Parameters

### ver

`ver` — column with the version number. Type `UInt*`, `Date`, `DateTime` or `DateTime64`. Optional parameter.

    When merging, `ReplacingMergeTree` from all the rows with the same sorting key leaves only one:

    -   The last in the selection, if `ver` not set. A selection is a set of rows in a set of parts participating in the merge. The most recently created part (the last insert) will be the last one in the selection. Thus, after deduplication, the very last row from the most recent insert will remain for each unique sorting key.
    -   With the maximum version, if `ver` specified. If `ver` is the same for several rows, then it will use "if `ver` is not specified" rule for them, i.e. the most recent inserted row will remain.

## Query clauses

When creating a `ReplacingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::warning
Do not use this method in new projects and, if possible, switch old projects to the method described above.
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

All of the parameters excepting `ver` have the same meaning as in `MergeTree`.

-   `ver` - column with the version. Optional parameter. For a description, see the text above.

</details>
