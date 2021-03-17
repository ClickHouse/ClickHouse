---
toc_priority: 141
---

# deltaSum {#agg_functions-deltasum}

Syntax: `deltaSum(value)`

Adds the differences between consecutive rows. If the difference is negative, it is ignored. 
`value` must be some integer or floating point type.

Note that the underlying data must be sorted in order for this function to work properly.
If you would like to use this function in a materialized view, you most likely want to use the
[deltaSumTimestamp](deltasumtimestamp.md) method instead. 

Example:

```sql
select deltaSum(arrayJoin([1, 2, 3]));                  -- => 2
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));   -- => 7
select deltaSum(arrayJoin([2.25, 3, 4.5]));             -- => 2.25
```

