---
slug: /en/sql-reference/aggregate-functions/reference/maxintersections
sidebar_position: 163
title: maxIntersections
---

# maxIntersections

Aggregate function that calculates the maximum number of times that a group of intervals intersects each other (if all the intervals intersect at least once).

The syntax is:

```sql
maxIntersections(start_column, end_column)
```

**Arguments**

- `start_column` – the numeric column that represents the start of each interval. If `start_column` is `NULL` or 0 then the interval will be skipped.

- `end_column` - the numeric column that represents the end of each interval. If `end_column` is `NULL` or 0 then the interval will be skipped.

**Returned value**

Returns the maximum number of intersected intervals.

**Example**

```sql
CREATE TABLE my_events (
    start UInt32,
    end UInt32
)
Engine = MergeTree
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

Three of these intervals have a common value (the value is `4`, but the value that is common is not important, we are measuring the count of the intersections). The intervals `(1,3)` and `(3,7)` share an endpoint but are not considered intersecting by the `maxIntersections` function.

```sql
SELECT maxIntersections(start, end) FROM my_events;
```

Response:
```response
3
```

If you have multiple occurrences of the maximum interval, you can use the [`maxIntersectionsPosition` function](./maxintersectionsposition.md) to locate the number and location of those occurrences.