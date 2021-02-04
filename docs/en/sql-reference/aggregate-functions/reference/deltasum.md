---
toc_priority: 141
---

# deltaSum {#agg_functions-deltasum}

Syntax: `deltaSum(value)`

Adds the differences between consecutive rows. If the difference is negative, it is ignored. 
`value` must be some integer, floating point or decimal type.


Example:

```sql
select deltaSum(arrayJoin([1, 2, 3]));                  -- => 3
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));   -- => 8
```

