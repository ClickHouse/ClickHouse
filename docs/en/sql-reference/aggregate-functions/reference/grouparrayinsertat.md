---
sidebar_position: 112
---

# groupArrayInsertAt {#grouparrayinsertat}

Inserts a value into the array at the specified position.

**Syntax**

``` sql
groupArrayInsertAt(default_x, size)(x, pos)
```

If in one query several values are inserted into the same position, the function behaves in the following ways:

-   If a query is executed in a single thread, the first one of the inserted values is used.
-   If a query is executed in multiple threads, the resulting value is an undetermined one of the inserted values.

**Arguments**

-   `x` — Value to be inserted. [Expression](../../../sql-reference/syntax.md#syntax-expressions) resulting in one of the [supported data types](../../../sql-reference/data-types/index.md).
-   `pos` — Position at which the specified element `x` is to be inserted. Index numbering in the array starts from zero. [UInt32](../../../sql-reference/data-types/int-uint.md#uint-ranges).
-   `default_x` — Default value for substituting in empty positions. Optional parameter. [Expression](../../../sql-reference/syntax.md#syntax-expressions) resulting in the data type configured for the `x` parameter. If `default_x` is not defined, the [default values](../../../sql-reference/statements/create/table.md#create-default-values) are used.
-   `size` — Length of the resulting array. Optional parameter. When using this parameter, the default value `default_x` must be specified. [UInt32](../../../sql-reference/data-types/int-uint.md#uint-ranges).

**Returned value**

-   Array with inserted values.

Type: [Array](../../../sql-reference/data-types/array.md#data-type-array).

**Example**

Query:

``` sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

Result:

``` text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

Result:

``` text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

Result:

``` text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

Multi-threaded insertion of elements into one position.

Query:

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

As a result of this query you get random integer in the `[0,9]` range. For example:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```
