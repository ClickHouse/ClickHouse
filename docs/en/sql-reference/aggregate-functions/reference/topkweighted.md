---
toc_priority: 109
---

# topKWeighted {#topkweighted}

Similar to `topK` but takes one additional argument of integer type - `weight`. Every value is accounted `weight` times for frequency calculation.

**Syntax**

``` sql
topKWeighted(N)(x, weight)
```

**Parameters**

-   `N` — The number of elements to return.

**Arguments**

-   `x` – The value.
-   `weight` — The weight. [UInt8](../../../sql-reference/data-types/int-uint.md).

**Returned value**

Returns an array of the values with maximum approximate sum of weights.

**Example**

Query:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

Result:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```
