---
slug: /en/engines/table-engines/mergetree-family/replacingmergetree
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
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [statement description](../../../sql-reference/statements/create/table.md).

:::note
Uniqueness of rows is determined by the `ORDER BY` table section, not `PRIMARY KEY`.
:::

## ReplacingMergeTree Parameters

### ver

`ver` — column with the version number. Type `UInt*`, `Date`, `DateTime` or `DateTime64`. Optional parameter.

When merging, `ReplacingMergeTree` from all the rows with the same sorting key leaves only one:

   - The last in the selection, if `ver` not set. A selection is a set of rows in a set of parts participating in the merge. The most recently created part (the last insert) will be the last one in the selection. Thus, after deduplication, the very last row from the most recent insert will remain for each unique sorting key.
   - With the maximum version, if `ver` specified. If `ver` is the same for several rows, then it will use "if `ver` is not specified" rule for them, i.e. the most recent inserted row will remain.

Example: 

```sql
-- without ver - the last inserted 'wins'
CREATE TABLE myFirstReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO myFirstReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO myFirstReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM myFirstReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ second  │ 2020-01-01 00:00:00 │
└─────┴─────────┴─────────────────────┘


-- with ver - the row with the biggest ver 'wins'
CREATE TABLE mySecondReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO mySecondReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO mySecondReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM mySecondReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ first   │ 2020-01-01 01:01:01 │
└─────┴─────────┴─────────────────────┘
```

### is_deleted

`is_deleted` —  Name of a column used during a merge to determine whether the data in this row represents the state or is to be deleted; `1` is a “deleted“ row, `0` is a “state“ row.

  Column data type — `UInt8`.

:::note
`is_deleted` can only be enabled when `ver` is used.

The row is deleted when `OPTIMIZE ... FINAL CLEANUP` or `OPTIMIZE ... FINAL` is used.

No matter the operation on the data, the version must be increased. If two inserted rows have the same version number, the last inserted row is the one kept.

:::

Example:
```sql
-- with ver and is_deleted
CREATE OR REPLACE TABLE myThirdReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(eventTime, is_deleted)
ORDER BY key;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 0);
INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 1); 

select * from myThirdReplacingMT final;

0 rows in set. Elapsed: 0.003 sec.

-- delete rows with is_deleted
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP; 

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final; 

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

## Query clauses

When creating a `ReplacingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
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

- `ver` - column with the version. Optional parameter. For a description, see the text above.

</details>
