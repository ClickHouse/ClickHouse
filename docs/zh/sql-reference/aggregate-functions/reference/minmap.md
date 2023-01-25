---
toc_priority: 142
---

# minMap {#agg_functions-minmap}

**语法**

```sql
minMap(key, value)
或
minMap(Tuple(key, value))
```

根据 `key` 数组中指定的键对 `value` 数组计算最小值。

传递 `key` 和 `value` 数组的元组与传递 `key` 和 `value` 的两个数组是同义的。
要总计的每一行的 `key` 和 `value` (数组)元素的数量必须相同。
返回两个数组组成的元组: 排好序的 `key`  和对应 `key` 的 `value` 计算值(最小值)。

**示例**

``` sql
SELECT minMap(a, b)
FROM values('a Array(Int32), b Array(Int64)', ([1, 2], [2, 2]), ([2, 3], [1, 1]))
```

``` text
┌─minMap(a, b)──────┐
│ ([1,2,3],[2,1,1]) │
└───────────────────┘
```
