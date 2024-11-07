---
slug: /en/sql-reference/aggregate-functions/reference/groupconcat
sidebar_position: 363
sidebar_label: groupConcat
title: groupConcat
---

Calculates a concatenated string from a group of strings, optionally separated by a delimiter, and optionally limited by a maximum number of elements.

**Syntax**

``` sql
groupConcat[(delimiter [, limit])](expression);
```

**Arguments**

- `expression` — The expression or column name that outputs strings to be concatenated..
- `delimiter` — A [string](../../../sql-reference/data-types/string.md) that will be used to separate concatenated values. This parameter is optional and defaults to an empty string if not specified.
- `limit` — A positive [integer](../../../sql-reference/data-types/int-uint.md) specifying the maximum number of elements to concatenate. If more elements are present, excess elements are ignored. This parameter is optional.

:::note
If delimiter is specified without limit, it must be the first parameter. If both delimiter and limit are specified, delimiter must precede limit.
:::

**Returned value**

- Returns a [string](../../../sql-reference/data-types/string.md) consisting of the concatenated values of the column or expression. If the group has no elements or only null elements, and the function does not specify a handling for only null values, the result is a nullable string with a null value.

**Examples**

Input table:

``` text
┌─id─┬─name─┐
│ 1  │  John│
│ 2  │  Jane│
│ 3  │   Bob│
└────┴──────┘
```

1.	Basic usage without a delimiter:

Query:

``` sql
SELECT groupConcat(Name) FROM Employees;
```

Result:

``` text
JohnJaneBob
```

This concatenates all names into one continuous string without any separator.


2. Using comma as a delimiter:

Query:

``` sql
SELECT groupConcat(', ')(Name)  FROM Employees;
```

Result:

``` text
John, Jane, Bob
```

This output shows the names separated by a comma followed by a space.


3. Limiting the number of concatenated elements

Query:

``` sql
SELECT groupConcat(', ', 2)(Name) FROM Employees;
```

Result:

``` text
John, Jane
```

This query limits the output to the first two names, even though there are more names in the table.
