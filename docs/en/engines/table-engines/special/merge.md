---
toc_priority: 36
toc_title: Merge
---

# Merge Table Engine {#merge}

The `Merge` engine (not to be confused with `MergeTree`) does not store data itself, but allows reading from any number of other tables simultaneously.

Reading is automatically parallelized. Writing to a table is not supported. When reading, the indexes of tables that are actually being read are used, if they exist.

The `Merge` engine accepts parameters: the database name and a regular expression for tables.

## Examples {#examples}

Example 1:

``` sql
Merge(hits, '^WatchLog')
```

Data will be read from the tables in the `hits` database that have names that match the regular expression ‘`^WatchLog`’.

Instead of the database name, you can use a constant expression that returns a string. For example, `currentDatabase()`.

Regular expressions — [re2](https://github.com/google/re2) (supports a subset of PCRE), case-sensitive.
See the notes about escaping symbols in regular expressions in the “match” section.

When selecting tables to read, the `Merge` table itself will not be selected, even if it matches the regex. This is to avoid loops.
It is possible to create two `Merge` tables that will endlessly try to read each others’ data, but this is not a good idea.

The typical way to use the `Merge` engine is for working with a large number of `TinyLog` tables as if with a single table.

Example 2:

Let’s say you have a old table (WatchLog_old) and decided to change partitioning without moving data to a new table (WatchLog_new) and you need to see data from both tables.

``` sql
CREATE TABLE WatchLog_old(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree(date, (UserId, EventType), 8192);
INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree PARTITION BY date ORDER BY (UserId, EventType) SETTINGS index_granularity=8192;
INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog as WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT *
FROM WatchLog
```

``` text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

## Virtual Columns {#virtual-columns}

-   `_table` — Contains the name of the table from which data was read. Type: [String](../../../sql-reference/data-types/string.md).

    You can set the constant conditions on `_table` in the `WHERE/PREWHERE` clause (for example, `WHERE _table='xyz'`). In this case the read operation is performed only for that tables where the condition on `_table` is satisfied, so the `_table` column acts as an index.

**See Also**

-   [Virtual columns](../../../engines/table-engines/special/index.md#table_engines-virtual_columns)

[Original article](https://clickhouse.com/docs/en/operations/table_engines/merge/) <!--hide-->
