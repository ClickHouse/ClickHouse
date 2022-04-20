---
sidebar_position: 108
---

# groupArraySorted {#groupArraySorted}

Returns an array with the first N items in ascending order.

``` sql
groupArraySorted(N)(column)
```

**Arguments**

-   `N` – The number of elements to return.

If the parameter is omitted, default value 10 is used.

**Arguments**

-   `column` – The value.
-   `expr` — Optional. The field or expresion to sort by. If not set values are sorted by themselves.

**Example**

Gets the first 10 numbers:

``` sql
SELECT groupArraySorted(10)(number) FROM numbers(100)
```

``` text
┌─groupArraySorted(10)(number)─┐
│ [0,1,2,3,4,5,6,7,8,9]        │
└──────────────────────────────┘
```

Or the last 10:

``` sql
SELECT groupArraySorted(10)(number, -number) FROM numbers(100)
```

``` text
┌─groupArraySorted(10)(number, negate(number))─┐
│ [99,98,97,96,95,94,93,92,91,90]              │
└──────────────────────────────────────────────┘
```