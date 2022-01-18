---
toc_priority: 141
---

# deltaSum {#agg_functions-deltasum}

Syntax: `deltaSum(value)`

Adds the differences between consecutive rows. If the difference is negative, it is ignored. 
`value` must be some integer or floating point type.

Example:

```sql
select deltaSum(arrayJoin([1, 2, 3]));                  -- => 2
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));   -- => 7
select deltaSum(arrayJoin([2.25, 3, 4.5])); -- => 2.25
```

