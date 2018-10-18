# ReplacingMergeTree

The engine differs from [MergeTree](mergetree.md#table_engines-mergetree) in that it removes duplicate entries with the same primary key value.

Data deduplication occurs only during a merge. Merging occurs in the background at an unknown time, so you can't plan for it. Some of the data may remain unprocessed. Although you can run an unscheduled merge using the `OPTIMIZE` query, don't count on using it, because the `OPTIMIZE` query will read and write a large amount of data.

Thus, `ReplacingMergeTree` is suitable for clearing out duplicate data  in the background in order to save space, but it doesn't guarantee the absence of duplicates.

## Creating a Table

```
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../query_language/create.md#query_language-queries-create_table).

**ReplacingMergeTree Parameters**

- `ver` — column with version. Type `UInt*`, `Date` or `DateTime`. Optional parameter.

    When merging, `ReplacingMergeTree` from all the rows with the same primary key leaves only one:
    - Last in the selection, if `ver` not set.
    - With the maximum version, if `ver` specified.

**Query clauses**

When creating a `ReplacingMergeTree` table the same [clauses](mergetree.md#table_engines-mergetree-configuring)  are required, as when creating a `MergeTree`  table.

### Deprecated Method for Creating a Table

!!! attention
    Do not use this method in new projects and, if possible, switch the old projects to the method described above.

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

All of the parameters excepting `ver` have the same meaning as in `MergeTree`.


- `ver` - column with the version. Optional parameter. For a description, see the text above.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/replacingmergetree/) <!--hide-->
