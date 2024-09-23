---
slug: /en/sql-reference/functions/ulid-functions
sidebar_position: 190
sidebar_label: ULID
---

# Functions for Working with ULID

## generateULID

Generates the [ULID](https://github.com/ulid/spec).

**Syntax**

``` sql
generateULID([x])
```

**Arguments**

- `x` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in any of the [supported data types](../data-types/index.md#data_types). The resulting value is discarded, but the expression itself if used for bypassing [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in one query. Optional parameter.

**Returned value**

The [FixedString](../data-types/fixedstring.md) type value.

**Usage example**

``` sql
SELECT generateULID()
```

``` text
┌─generateULID()─────────────┐
│ 01GNB2S2FGN2P93QPXDNB4EN2R │
└────────────────────────────┘
```

**Usage example if it is needed to generate multiple values in one row**

```sql
SELECT generateULID(1), generateULID(2)
```

``` text
┌─generateULID(1)────────────┬─generateULID(2)────────────┐
│ 01GNB2SGG4RHKVNT9ZGA4FFMNP │ 01GNB2SGG4V0HMQVH4VBVPSSRB │
└────────────────────────────┴────────────────────────────┘
```

## ULIDStringToDateTime

This function extracts the timestamp from a ULID.

**Syntax**

``` sql
ULIDStringToDateTime(ulid[, timezone])
```

**Arguments**

- `ulid` — Input ULID. [String](../data-types/string.md) or [FixedString(26)](../data-types/fixedstring.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). [String](../data-types/string.md).

**Returned value**

- Timestamp with milliseconds precision. [DateTime64(3)](../data-types/datetime64.md).

**Usage example**

``` sql
SELECT ULIDStringToDateTime('01GNB2S2FGN2P93QPXDNB4EN2R')
```

``` text
┌─ULIDStringToDateTime('01GNB2S2FGN2P93QPXDNB4EN2R')─┐
│                            2022-12-28 00:40:37.616 │
└────────────────────────────────────────────────────┘
```

## See Also

- [UUID](../../sql-reference/functions/uuid-functions.md)
