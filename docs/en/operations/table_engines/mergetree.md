<a name="table_engines-mergetree"></a>

# MergeTree

The MergeTree engine supports an index by primary key and by date, and provides the possibility to update data in real time.
This is the most advanced table engine in ClickHouse. Don't confuse it with the Merge engine.

The engine accepts parameters: the name of a Date type column containing the date, a sampling expression (optional), a tuple that defines the table's primary key, and the index granularity.

Example without sampling support.

```text
MergeTree(EventDate, (CounterID, EventDate), 8192)
```

Example with sampling support.

```text
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

A MergeTree table must have a separate column containing the date. Here, it is the EventDate column. The date column must have the 'Date' type (not 'DateTime').

The primary key may be a tuple from any expressions (usually this is just a tuple of columns), or a single expression.

The sampling expression (optional) can be any expression. It must also be present in the primary key. The example uses a hash of user IDs to pseudo-randomly disperse data in the table for each CounterID and EventDate. In other words, when using the SAMPLE clause in a query, you get an evenly pseudo-random sample of data for a subset of users.

The table is implemented as a set of parts. Each part is sorted by the primary key. In addition, each part has the minimum and maximum date assigned. When inserting in the table, a new sorted part is created. The merge process is periodically initiated in the background. When merging, several parts are selected (usually the smallest ones) and then merged into one large sorted part.

In other words, incremental sorting occurs when inserting to the table. Merging is implemented so that the table always consists of a small number of sorted parts, and the merge itself doesn't do too much work.

During insertion, data belonging to different months is separated into different parts. The parts that correspond to different months are never combined. The purpose of this is to provide local data modification (for ease in backups).

Parts are combined up to a certain size threshold, so there aren't any merges that are too long.

For each part, an index file is also written. The index file contains the primary key value for every 'index_granularity' row in the table. In other words, this is an abbreviated index of sorted data.

For columns, "marks" are also written to each 'index_granularity' row so that data can be read in a specific range.

When reading from a table, the SELECT query is analyzed for whether indexes can be used.
An index can be used if the WHERE or PREWHERE clause has an expression (as one of the conjunction elements, or entirely) that represents an equality or inequality comparison operation, or if it has IN or LIKE with a fixed prefix on columns or expressions that are in the primary key or partitioning key, or on certain partially repetitive functions of these columns, or logical relationships of these expressions.

Thus, it is possible to quickly run queries on one or many ranges of the primary key. In this example, queries will be fast when run for a specific tracking tag; for a specific tag and date range; for a specific tag and date; for multiple tags with a date range, and so on.

```sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

All of these cases will use the index by date and by primary key. The index is used even for complex expressions. Reading from the table is organized so that using the index can't be slower than a full scan.

In this example, the index can't be used.

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

To check whether ClickHouse can use the index when executing the query, use the settings  [force_index_by_date](../settings/settings.md#settings-settings-force_index_by_date)and[force_primary_key](../settings/settings.md#settings-settings-force_primary_key).

The index by date only allows reading those parts that contain dates from the desired range. However, a data part may contain data for many dates (up to an entire month), while within a single part the data is ordered by the primary key, which might not contain the date as the first column. Because of this, using a query with only a date condition that does not specify the primary key prefix will cause more data to be read than for a single date.

For concurrent table access, we use multi-versioning. In other words, when a table is simultaneously read and updated, data is read from a set of parts that is current at the time of the query. There are no lengthy locks. Inserts do not get in the way of read operations.

Reading from a table is automatically parallelized.

The `OPTIMIZE` query is supported, which calls an extra merge step.

You can use a single large table and continually add data to it in small chunks – this is what MergeTree is intended for.

Data replication is possible for all types of tables in the MergeTree family (see the section "Data replication").

