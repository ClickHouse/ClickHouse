<a name="table_engine-collapsingmergetree"></a>

# CollapsingMergeTree

*This engine is used specifically for Yandex.Metrica.*

It differs from `MergeTree` in that it allows automatic deletion, or "collapsing" certain pairs of rows when merging.

Yandex.Metrica has normal logs (such as hit logs) and change logs. Change logs are used for incrementally calculating statistics on data that is constantly changing. Examples are the log of session changes, or logs of changes to user histories. Sessions are constantly changing in Yandex.Metrica. For example, the number of hits per session increases. We refer to changes in any object as a pair (?old values, ?new values). Old values may be missing if the object was created. New values may be missing if the object was deleted. If the object was changed, but existed previously and was not deleted, both values are present. In the change log, one or two entries are made for each change. Each entry contains all the attributes that the object has, plus a special attribute for differentiating between the old and new values. When objects change, only the new entries are added to the change log, and the existing ones are not touched.

The change log makes it possible to incrementally calculate almost any statistics. To do this, we need to consider "new" rows with a plus sign, and "old" rows with a minus sign. In other words, incremental calculation is possible for all statistics whose algebraic structure contains an operation for taking the inverse of an element. This is true of most statistics. We can also calculate "idempotent" statistics, such as the number of unique visitors, since the unique visitors are not deleted when making changes to sessions.

This is the main concept that allows Yandex.Metrica to work in real time.

CollapsingMergeTree accepts an additional parameter - the name of an Int8-type column that contains the row's "sign". Example:

```sql
CollapsingMergeTree(EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign)
```

Here, `Sign` is a column containing -1 for "old" values and 1 for "new" values.

When merging, each group of consecutive identical primary key values (columns for sorting data) is reduced to no more than one row with the column value 'sign_column = -1' (the "negative row") and no more than one row with the column value 'sign_column = 1' (the "positive row"). In other words, entries from the change log are collapsed.

If the number of positive and negative rows matches, the first negative row and the last positive row are written.
If there is one more positive row than negative rows, only the last positive row is written.
If there is one more negative row than positive rows, only the first negative row is written.
Otherwise, there will be a logical error and none of the rows will be written. (A logical error can occur if the same section of the log was accidentally inserted more than once. The error is just recorded in the server log, and the merge continues.)

Thus, collapsing should not change the results of calculating statistics.
Changes are gradually collapsed so that in the end only the last value of almost every object is left.
Compared to MergeTree, the CollapsingMergeTree engine allows a multifold reduction of data volume.

There are several ways to get completely "collapsed" data from a `CollapsingMergeTree` table:

1. Write a query with GROUP BY and aggregate functions that accounts for the sign. For example, to calculate quantity, write 'sum(Sign)' instead of 'count()'. To calculate the sum of something, write 'sum(Sign * x)' instead of 'sum(x)', and so on, and also add 'HAVING sum(Sign) `>` 0'. Not all amounts can be calculated this way. For example, the aggregate functions 'min' and 'max' can't be rewritten.
2. If you must extract data without aggregation (for example, to check whether rows are present whose newest values match certain conditions), you can use the FINAL modifier for the FROM clause. This approach is significantly less efficient.

