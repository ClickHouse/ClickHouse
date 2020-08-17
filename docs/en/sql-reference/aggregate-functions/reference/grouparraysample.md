---
toc_priority: 114
---

# groupArraySample {#grouparraysample}

Creates an array of sample argument values. The size of the resulting array is limited to `max_size` elements. Argument values are selected and added to the array randomly. 

**Syntax**

``` sql
groupArraySample(max_size)(x)
```
or
``` sql
groupArraySample(max_size, seed)(x)
```

**Parameters**

-   `max_size` — Maximum size of the resulting array. Positive [UInt64](../../data-types/int-uint.md).
-   `seed` — Seed for the random number generator. Optional, can be omitted. Positive [UInt64](../../data-types/int-uint.md). Default value: `123456`.
-   `x` — Argument name. [String](../../data-types/string.md).

**Returned values**

-   Array of randomly selected `x` arguments.

Type: [Array](../../data-types/array.md).

**Examples**

Consider table `colors`:

``` text
┌─id─┬─color──┐
│  1 │ red    │
│  2 │ blue   │
│  3 │ green  │
│  4 │ white  │
│  5 │ orange │
└────┴────────┘
```

Select `id`-s query:

``` sql
SELECT groupArraySample(3)(id) FROM colors;
```

Result:

``` text
┌─groupArraySample(3)(id)─┐
│ [1,2,4]                 │
└─────────────────────────┘
```

Select `color`-s query:

``` sql
SELECT groupArraySample(3)(color) FROM colors;
```

Result:

```text
┌─groupArraySample(3)(color)─┐
│ ['white','blue','green']   │
└────────────────────────────┘
```

Select `color`-s query with different seed:

``` sql
SELECT groupArraySample(3, 987654321)(color) FROM colors;
```

Result:

```text
┌─groupArraySample(3, 987654321)(color)─┐
│ ['red','orange','green']              │
└───────────────────────────────────────┘
```
