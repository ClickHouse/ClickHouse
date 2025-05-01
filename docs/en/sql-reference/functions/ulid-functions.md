---
description: 'Documentation for Functions for Working with ULID'
sidebar_label: 'ULID'
sidebar_position: 190
slug: /sql-reference/functions/ulid-functions
title: 'Functions for Working with ULID'
---

# Functions for Working with ULID

## generateULID {#generateulid}

Generates the [ULID](https://github.com/ulid/spec).

**Syntax**

```sql
generateULID([x])
```

**Arguments**

- `x` — [Expression](/sql-reference/syntax#expressions) resulting in any of the [supported data types](/sql-reference/data-types). The resulting value is discarded, but the expression itself if used for bypassing [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) if the function is called multiple times in one query. Optional parameter.

**Returned value**

The [FixedString](../data-types/fixedstring.md) type value.

**Usage example**

```sql
SELECT generateULID()
```

```text
┌─generateULID()─────────────┐
│ 01GNB2S2FGN2P93QPXDNB4EN2R │
└────────────────────────────┘
```

**Usage example if it is needed to generate multiple values in one row**

```sql
SELECT generateULID(1), generateULID(2)
```

```text
┌─generateULID(1)────────────┬─generateULID(2)────────────┐
│ 01GNB2SGG4RHKVNT9ZGA4FFMNP │ 01GNB2SGG4V0HMQVH4VBVPSSRB │
└────────────────────────────┴────────────────────────────┘
```

## ULIDStringToDateTime {#ulidstringtodatetime}

This function extracts the timestamp from a ULID.

**Syntax**

```sql
ULIDStringToDateTime(ulid[, timezone])
```

**Arguments**

- `ulid` — Input ULID. [String](../data-types/string.md) or [FixedString(26)](../data-types/fixedstring.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). [String](../data-types/string.md).

**Returned value**

- Timestamp with milliseconds precision. [DateTime64(3)](../data-types/datetime64.md).

**Usage example**

```sql
SELECT ULIDStringToDateTime('01GNB2S2FGN2P93QPXDNB4EN2R')
```

```text
┌─ULIDStringToDateTime('01GNB2S2FGN2P93QPXDNB4EN2R')─┐
│                            2022-12-28 00:40:37.616 │
└────────────────────────────────────────────────────┘
```

## See Also {#see-also}

- [UUID](../../sql-reference/functions/uuid-functions.md)
