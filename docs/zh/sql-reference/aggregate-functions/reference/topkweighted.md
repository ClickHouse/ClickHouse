---
toc_priority: 109
---

# topKWeighted {#topkweighted}

类似于 `topK`  但需要一个整数类型的附加参数 - `weight`。 每个输入都被记入 `weight` 次频率计算。

**语法**

``` sql
topKWeighted(N)(x, weight)
```

**参数**

-   `N` — 要返回的元素数。

**参数**

-   `x` – (要计算频次的)值。
-   `weight` — 权重。 [UInt8](../../../sql-reference/data-types/int-uint.md)类型。

**返回值**

返回具有最大近似权重总和的值数组。

**示例**

查询:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

结果:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```
