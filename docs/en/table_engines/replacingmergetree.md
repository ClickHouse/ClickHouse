# ReplacingMergeTree

This engine table differs from `MergeTree` in that it removes duplicate entries with the same primary key value.

The last optional parameter for the table engine is the "version" column. When merging, it reduces all rows with the same primary key value to just one row. If the version column is specified, it leaves the row with the highest version; otherwise, it leaves the last row.

The version column must have a type from the `UInt` family, `Date`, or `DateTime`.

```sql
ReplacingMergeTree(EventDate, (OrderID, EventDate, BannerID, ...), 8192, ver)
```

Note that data is only deduplicated during merges. Merging occurs in the background at an unknown time, so you can't plan for it. Some of the data may remain unprocessed. Although you can run an unscheduled merge using the OPTIMIZE query, don't count on using it, because the OPTIMIZE query will read and write a large amount of data.

Thus, `ReplacingMergeTree` is suitable for clearing out duplicate data  in the background in order to save space, but it doesn't guarantee the absence of duplicates.

*This engine is not used in Yandex.Metrica, but it has been applied in other Yandex projects.*

