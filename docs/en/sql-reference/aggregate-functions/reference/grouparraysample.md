---
sidebar_position: 114
---

# groupArraySample {#grouparraysample}

Creates an array of sample argument values. The size of the resulting array is limited to `max_size` elements. Argument values are selected and added to the array randomly.

**Syntax**

``` sql
groupArraySample(max_size[, seed])(x)
```

**Arguments**

-   `max_size` — Maximum size of the resulting array. [UInt64](../../data-types/int-uint.md).
-   `seed` — Seed for the random number generator. Optional. [UInt64](../../data-types/int-uint.md). Default value: `123456`.
-   `x` — Argument (column name or expression).

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

Query with column name as argument:

``` sql
SELECT groupArraySample(3)(color) as newcolors FROM colors;
```

Result:

```text
┌─newcolors──────────────────┐
│ ['white','blue','green']   │
└────────────────────────────┘
```

Query with column name and different seed:

``` sql
SELECT groupArraySample(3, 987654321)(color) as newcolors FROM colors;
```

Result:

```text
┌─newcolors──────────────────┐
│ ['red','orange','green']   │
└────────────────────────────┘
```

Query with expression as argument:

``` sql
SELECT groupArraySample(3)(concat('light-', color)) as newcolors FROM colors;
```

Result:

```text
┌─newcolors───────────────────────────────────┐
│ ['light-blue','light-orange','light-green'] │
└─────────────────────────────────────────────┘
```
