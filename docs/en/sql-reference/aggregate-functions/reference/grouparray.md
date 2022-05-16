---
sidebar_position: 110
---

# groupArray {#agg_function-grouparray}

Syntax: `groupArray(x)` or `groupArray(max_size)(x)`

Creates an array of argument values.
Values can be added to the array in any (indeterminate) order.

The second version (with the `max_size` parameter) limits the size of the resulting array to `max_size` elements. For example, `groupArray(1)(x)` is equivalent to `[any (x)]`.

In some cases, you can still rely on the order of execution. This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY`.

**Example**

``` text
SELECT * FROM default.ck;

┌─id─┬─name─────┐
│  1 │ zhangsan │
│  1 │ ᴺᵁᴸᴸ     │
│  1 │ lisi     │
│  2 │ wangwu   │
└────┴──────────┘

```

Query:

``` sql
select id, groupArray(10)(name) from default.ck group by id;
```

Result:

``` text
┌─id─┬─groupArray(10)(name)─┐
│  1 │ ['zhangsan','lisi']  │
│  2 │ ['wangwu']           │
└────┴──────────────────────┘
```

The groupArray function will remove ᴺᵁᴸᴸ value based on the above results.
