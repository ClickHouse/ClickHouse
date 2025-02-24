---
slug: /en/sql-reference/functions/comparison-functions
sidebar_position: 35
sidebar_label: Comparison
---

# Comparison Functions

The comparison functions below return `0` or `1` with type [UInt8](/docs/en/sql-reference/data-types/int-uint). Only values within the same group can be compared (e.g. `UInt16` and `UInt64`) but not across groups (e.g. `UInt16` and `DateTime`). Comparison of numbers and strings are possible, as is comparison of strings with dates and dates with times. For tuples and arrays, the comparison is lexicographic meaning that the comparison is made for each corresponding element of the left side and right side tuple/array. 

The following types can be compared:
- numbers and decimals
- strings and fixed strings
- dates
- dates with times
- tuples (lexicographic comparison)
- arrays (lexicographic comparison)

:::note
Strings are compared byte-by-byte. This may lead to unexpected results if one of the strings contains UTF-8 encoded multi-byte characters.
A string S1 which has another string S2 as prefix is considered longer than S2.
:::

## equals, `=`, `==` operators {#equals}

**Syntax**

```sql
equals(a, b)
```

Alias:
- `a = b` (operator)
- `a == b` (operator)

## notEquals, `!=`, `<>` operators {#notequals}

**Syntax**

```sql
notEquals(a, b)
```

Alias:
- `a != b` (operator)
- `a <> b` (operator)

## less, `<` operator {#less}

**Syntax**

```sql
less(a, b)
```

Alias:
- `a < b` (operator)

## greater, `>` operator {#greater}

**Syntax**

```sql
greater(a, b)
```

Alias:
- `a > b` (operator)

## lessOrEquals, `<=` operator {#lessorequals}

**Syntax**

```sql
lessOrEquals(a, b)
```

Alias:
- `a <= b` (operator)

## greaterOrEquals, `>=` operator {#greaterorequals}

**Syntax**

```sql
greaterOrEquals(a, b)
```

Alias:
- `a >= b` (operator)
