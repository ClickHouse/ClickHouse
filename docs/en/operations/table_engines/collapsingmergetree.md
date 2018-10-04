<a name="table_engine-collapsingmergetree"></a>

# CollapsingMergeTree

The engine inherits from [MergeTree](mergetree.md#table_engines-mergetree) and adds to data parts merge algorithm the logic of rows collapsing.

`CollapsingMergeTree` deletes rows collapsing certain pairs of them into one row. The engine may significantly reduce the volume of storage and efficiency of `SELECT` query as a consequence.

## Creating a Table

```
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CollapsingMergeTree(sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../query_language/create.md#query_language-queries-create_table).

**CollapsingMergeTree Parameters**

- `sign` — column with a sign of event.

    Type — `Int8`. Possible values are 1 and -1.

**Query clauses**

When creating a `CollapsingMergeTree` table the same [clauses](mergetree.md#table_engines-mergetree-configuring) are required, as when creating a `MergeTree` table.

### Deprecated Method for Creating a Table

!!!attention
    Do not use this method in new projects and, if possible, switch the old projects to the method described above.

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign)
```

All of the parameters excepting `sign` have the same meaning as in `MergeTree`.

- `sign` — column with a sign of event.

    Type — `Int8`. Possible values are 1 and -1.

## Collapsing

### Data

`CollapsingMergeTree` works effectively with the data represented in the form of change log. Change logs are used for incrementally calculating statistics on the data that is constantly changing. Examples are the log of session changes, or logs of changes to user histories. Each row of such a log describes some object and contains additional `sign` field that indicates the values in this row are intended for increment or for decrement. If object is deleted then `sign = -1`, if object is created, then `sign = 1`, if object is changed `sign` can be `1` or `-1` depending on the kind of change.

### Algorithm

When ClickHouse merges data parts, each group of consecutive rows with identical primary key is reduced to not more than two rows, one with `sign = 1` ("increment" row) and another with `sign = -1` ("decrement" row). In other words, entries are collapsed for each resulting data part.

ClickHouse saves:

  1. The first "decrement" and the last "increment" rows, if the number of "increment" and "decrement" rows matches.
  1. The last "increment" row, if there is one more "increment" row than "decrement" rows.
  1. The first "decrement" row, if there is one more "decrement" row than "increment" rows.
  1. None of rows, in all other cases.

      ClickHouse treats this situation as a logical error and record it in the server log, the merge continues. This error can occur if the same data were accidentally inserted more than once.

!!! Я так и не понял, что в точности происходит при коллапсировании. Что происходит с данными в строке, которые не в первичном ключе и не sign? Какого они должны быть типа или что вообще должно происходить, чтобы при схлопывании рассчитывалась инкрементальная статистика?

Thus, collapsing should not change the results of calculating statistics.
Changes are gradually collapsed so that in the end only the last value of almost every object is left.

Merging algorithm doesn't guarantee than all of the rows with the same primary key will be in one resulting data part. This means that additional aggregation is required if there is a need to get completely "collapsed" data from `CollapsingMergeTree` table.

Ways to complete aggregation:

1. Write a query with `GROUP BY` clause and aggregate functions that accounts for the sign. For example, to calculate quantity, use `sum(sign)` instead of `count()`. To calculate the sum of something, use `sum(sign * x)` instead of `sum(x)`, and so on, and also add `HAVING sum(sign) > 0`. Not all amounts can be calculated this way. For example, the aggregate functions `min` and `max` can't be rewritten.
2. If you must extract data without aggregation (for example, to check whether rows are present whose newest values match certain conditions), you can use the `FINAL` modifier for the `FROM` clause. This approach is significantly less efficient.

## Use in Yandex.Metrica

Yandex.Metrica has normal logs (such as hit logs) and change logs. Change logs are used for incrementally calculating statistics on data that is constantly changing. Examples are the log of session changes, or logs of changes to user histories. Sessions are constantly changing in Yandex.Metrica. For example, the number of hits per session increases. We refer to changes in any object as a pair (?old values, ?new values). Old values may be missing if the object was created. New values may be missing if the object was deleted. If the object was changed, but existed previously and was not deleted, both values are present. In the change log, one or two entries are made for each change. Each entry contains all the attributes that the object has, plus a special attribute for differentiating between the old and new values. When objects change, only the new entries are added to the change log, and the existing ones are not touched.

The change log makes it possible to incrementally calculate almost any statistics. To do this, we need to consider "new" rows with a plus sign, and "old" rows with a minus sign. In other words, incremental calculation is possible for all statistics whose algebraic structure contains an operation for taking the inverse of an element. This is true of most statistics. We can also calculate "idempotent" statistics, such as the number of unique visitors, since the unique visitors are not deleted when making changes to sessions.

This is the main concept that allows Yandex.Metrica to work in real time.
