---
toc_priority: 141
---

# deltaSum {#agg_functions-deltasum}

**语法**

``` sql
deltaSum(value)
```

计算连续行之间的差值和。如果差值为负，则忽略。
`value`必须是整型或浮点类型。

示例:

```sql
select deltaSum(arrayJoin([1, 2, 3]));                  -- => 2
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));   -- => 7
select deltaSum(arrayJoin([2.25, 3, 4.5])); -- => 2.25
```

