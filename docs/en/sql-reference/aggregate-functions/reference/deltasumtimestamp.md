---
toc_priority: 141
---

# deltaSumTimestamp {#agg_functions-deltasum}

Syntax: `deltaSumTimestamp(value, timestamp)`

Adds the differences between consecutive rows. If the difference is negative, it is ignored. 
Uses `timestamp` to order values. 
`value` must be some integer or floating point type or a Date or DateTime.
`timestamp` must be some integer or floating point type or a Date or DateTime.

This function works better in materialized views that are ordered by some time bucket aligned
timestamp, for example a `toStartOfMinute` bucket. Because the rows in such a materialized view
will all have the same timestamp, it is impossible for them to be merged in the "right" order. This
function keeps track of the `timestamp` of the values it's seen, so it's possible to order the states
correctly during merging.

Example:

```sql
select deltaSumTimestamp(value, timestamp) from (select number as timestamp, [0, 4, 8, 3, 0, 0, 0, 1, 3, 5][number] as value from numbers(1, 10));  -- => 13
```

