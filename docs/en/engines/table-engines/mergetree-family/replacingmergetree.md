---
toc_priority: 33
toc_title: ReplacingMergeTree
---

# ReplacingMergeTree {#replacingmergetree}

The engine differs from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree) in that it removes duplicate entries with the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md) value.

Data deduplication occurs only during a merge. Merging occurs in the background at an unknown time, so you can’t plan for it. Some of the data may remain unprocessed. Although you can run an unscheduled merge using the `OPTIMIZE` query, don’t count on using it, because the `OPTIMIZE` query will read and write a large amount of data.

Thus, `ReplacingMergeTree` is suitable for clearing out duplicate data in the background in order to save space, but it doesn’t guarantee the absence of duplicates.

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

**ReplacingMergeTree Parameters**

-   `ver` — column with version. Type `UInt*`, `Date` or `DateTime`. Optional parameter.

    When merging, `ReplacingMergeTree` from all the rows with the same sorting key leaves only one:

    -   Last in the selection, if `ver` not set.
    -   With the maximum version, if `ver` specified.

**Query clauses**

When creating a `ReplacingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

!!! attention "Attention"
    Do not use this method in new projects and, if possible, switch the old projects to the method described above.

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

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/replacingmergetree/) <!--hide-->
