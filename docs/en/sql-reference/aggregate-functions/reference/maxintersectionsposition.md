---
description: 'Aggregate function that calculates the positions of the occurrences
  of the maxIntersections function.'
sidebar_position: 164
slug: /sql-reference/aggregate-functions/reference/maxintersectionsposition
title: 'maxIntersectionsPosition'
---

# maxIntersectionsPosition

Aggregate function that calculates the positions of the occurrences of the [`maxIntersections` function](./maxintersections.md).

The syntax is:

```sql
maxIntersectionsPosition(start_column, end_column)
```

**Arguments**

- `start_column` – the numeric column that represents the start of each interval. If `start_column` is `NULL` or 0 then the interval will be skipped.

- `end_column` - the numeric column that represents the end of each interval. If `end_column` is `NULL` or 0 then the interval will be skipped.

**Returned value**

Returns the start positions of the maximum number of intersected intervals.

**Example**

```sql
CREATE TABLE my_events (
    start UInt32,
    end UInt32
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO my_events VALUES
   (1, 3),
   (1, 6),
   (2, 5),
   (3, 7);
```

The intervals look like the following:

```response
1 - 3
1 - - - - 6
  2 - - 5
    3 - - - 7
```

Notice that three of these intervals have the value 4 in common, and that starts with the 2nd interval:

```sql
SELECT maxIntersectionsPosition(start, end) FROM my_events;
```

Response:
```response
2
```

In other words, the `(1,6)` row is the start of the 3 intervals that intersect, and 3 is the maximum number of intervals that intersect.