---
sidebar_position: 141
---

# deltaSum {#agg_functions-deltasum}

计算连续行之间的差值和。如果差值为负，则忽略。

**语法**

``` sql
deltaSum(value)
```

**参数**

-   `value` — 必须是 [整型](../../data-types/int-uint.md) 或者 [浮点型](../../data-types/float.md) 。

**返回值**

- `Integer` or `Float` 型的算术差值和。

**示例**

查询:

``` sql
SELECT deltaSum(arrayJoin([1, 2, 3]));
```

结果:

``` text
┌─deltaSum(arrayJoin([1, 2, 3]))─┐
│                              2 │
└────────────────────────────────┘
```

查询:

``` sql
SELECT deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));
```

结果:

``` text
┌─deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]))─┐
│                                             7 │
└───────────────────────────────────────────────┘
```

查询:

``` sql
SELECT deltaSum(arrayJoin([2.25, 3, 4.5]));
```

结果:

``` text
┌─deltaSum(arrayJoin([2.25, 3, 4.5]))─┐
│                                2.25 │
└─────────────────────────────────────┘
```

**参见**

-   [runningDifference](../../functions/other-functions.md#other_functions-runningdifference)
