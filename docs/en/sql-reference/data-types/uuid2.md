---
description: 'Documentation for the UUID2 data type in ClickHouse'
sidebar_label: 'UUID2'
sidebar_position: 25
slug: /sql-reference/data-types/uuid2
title: 'UUID2'
doc_type: 'reference'
---

`UUID2` is a variant of the [UUID](/sql-reference/data-types/uuid) data type with correct sorting.

For historical reasons, the `UUID` data type is sorted by the second half of the value. This is unexpected and, in particular, hurts the performance of primary indexes built on `UUIDv7` columns, whose most significant bits are a timestamp (see the note in the [UUID](/sql-reference/data-types/uuid) documentation for details).

`UUID2` stores the value so that it is sorted by its textual (lexicographic) representation, which matches the canonical byte order used by most other systems. In every other respect it is compatible with `UUID`: it accepts the same textual representation, occupies the same 16 bytes, and supports the same set of functions.

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

## Sorting {#sorting}

The following example shows the difference in ordering between `UUID` and `UUID2` for the same set of `UUIDv7` values (whose first half is a timestamp):

```sql title="Query"
CREATE TABLE tab (uuid UUID2) ENGINE = MergeTree ORDER BY uuid;

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(10);
SELECT * FROM tab ORDER BY uuid;
```

The values are returned in the order of their textual representation. With a `UUID` column, the same query would return the values ordered by their second half instead.

## Selecting between UUID and UUID2 {#selecting-between-uuid-and-uuid2}

The name `UUID` resolves to either the `UUID` type (version 1, the historical behavior) or the `UUID2` type (version 2), controlled by the [`uuid_type_version`](/operations/settings/settings#uuid_type_version) setting:

```sql
SET uuid_type_version = 2;
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree ORDER BY uuid; -- `uuid` is materialized as `UUID2`
```

The resolved concrete type is materialized in the table definition, so reading an existing table does not depend on the value of the setting.

The explicit type names are not affected by the setting:

- `UUID1` is an alias of `UUID` (version 1).
- `UUID2` always refers to the correctly-sorting type.

## Converting between UUID and UUID2 {#converting-between-uuid-and-uuid2}

`UUID` and `UUID2` are distinct types, so conversion between them is explicit:

```sql
SELECT CAST(generateUUIDv4() AS UUID2);
SELECT CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID AS UUID2);
```

A `UUID2` value can be converted to and from `String`, `FixedString(16)` (canonical big-endian byte order) and `UInt128` (the plain integer value).

## Formats {#formats}

In the [Arrow](/interfaces/formats/Arrow) and [Parquet](/interfaces/formats/Parquet) formats, both `UUID` and `UUID2` are written as the standard UUID type of the corresponding format (the `arrow.uuid` extension type / the parquet `UUID` logical type) with the same canonical bytes, so other systems read the two ClickHouse types identically. The file additionally records which columns were written from `UUID2` (Arrow field metadata `ClickHouse:type`; parquet footer key-value metadata `ClickHouse:uuid2_leaf_columns`), and ClickHouse schema inference uses it to restore the exact type on a round-trip. An explicitly specified schema takes precedence over the recorded type.

## Related content {#related-content}

- [UUID](/sql-reference/data-types/uuid)
- [Functions for working with UUID](/sql-reference/functions/uuid-functions)
