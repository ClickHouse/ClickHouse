# Proposed SQL Syntax for Per-Subcolumn Codecs

## Principles

- Preserve the existing top-level `CODEC` syntax.
- Make per-element declarations visually local to the corresponding tuple element in `CREATE TABLE`.
- Use an explicit `SUBCOLUMN` keyword in `ALTER TABLE` to distinguish a tuple element from a physical top-level column whose name contains a dot.
- Keep codec declarations as storage metadata even though the `CREATE TABLE` syntax places them inside the tuple declaration.

## `CREATE TABLE`

Annotate tuple elements directly:

```sql
CREATE TABLE events
(
    id UInt64,

    payload Tuple(
        timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
        value Float64 CODEC(Gorilla, ZSTD(1)),
        tag String CODEC(ZSTD(3))
    ) CODEC(LZ4)
)
ENGINE = MergeTree
ORDER BY id;
```

The outer `CODEC(LZ4)` is the codec inherited by elements without a more specific declaration. Each inner declaration overrides it for that element.

For example:

```sql
payload Tuple(
    timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD),
    value Float64,
    metadata Tuple(
        source String CODEC(ZSTD(3)),
        sequence UInt64
    )
) CODEC(LZ4)
```

The effective codecs are:

| Subcolumn | Effective codec |
|---|---|
| `payload.timestamp` | `DoubleDelta, ZSTD` |
| `payload.value` | `LZ4` |
| `payload.metadata.source` | `ZSTD(3)` |
| `payload.metadata.sequence` | `LZ4` |

Without an outer declaration, elements that do not declare a codec use the part default:

```sql
payload Tuple(
    timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD),
    value Float64,
    tag String CODEC(ZSTD(3))
)
```

Unnamed tuple elements use the same syntax:

```sql
pair Tuple(
    UInt64 CODEC(Delta, ZSTD),
    String CODEC(ZSTD)
)
```

Although the declaration appears inside `Tuple`, it is not part of the `DataTypeTuple` identity. For example, the logical type of both of these columns remains `Tuple(UInt64, String)` regardless of the selected codecs.

## `ALTER TABLE`

Retain the existing syntax for a top-level column:

```sql
ALTER TABLE events
    MODIFY COLUMN payload CODEC(ZSTD(1));
```

Introduce `MODIFY SUBCOLUMN` for a tuple element:

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN payload.timestamp
    CODEC(DoubleDelta, ZSTD(1));
```

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN payload.value
    CODEC(Gorilla, ZSTD(1));
```

Multiple subcolumns can be changed in one query:

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN payload.timestamp CODEC(DoubleDelta, ZSTD(1)),
    MODIFY SUBCOLUMN payload.value CODEC(Gorilla, ZSTD(1)),
    MODIFY SUBCOLUMN payload.tag CODEC(ZSTD(3));
```

Nested tuple elements use a compound path:

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN payload.metadata.source
    CODEC(ZSTD(5));
```

For positional tuple elements, use a quoted numeric path segment:

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN pair.`1`
    CODEC(Delta, ZSTD);
```

A quoted segment also permits a tuple element name that contains a dot:

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN payload.`metadata.source`
    CODEC(ZSTD(5));
```

## Removing a Declaration

Remove the codec declared specifically for a subcolumn:

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN payload.timestamp REMOVE CODEC;
```

The subcolumn then inherits the nearest ancestor codec. If no ancestor declares a codec, it uses the part default.

The existing root form remains unchanged:

```sql
ALTER TABLE events
    MODIFY COLUMN payload REMOVE CODEC;
```

## Explicit `Default`

Use `CODEC(Default)` when a subcolumn must use the part default instead of inheriting an explicit parent codec:

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN payload.timestamp CODEC(Default);
```

For example:

```sql
payload Tuple(
    timestamp DateTime64(3) CODEC(Default),
    value Float64
) CODEC(ZSTD(3))
```

Here `payload.timestamp` uses the part default, while `payload.value` inherits `ZSTD(3)`.

## Name Disambiguation

`MODIFY SUBCOLUMN` is intentionally distinct from `MODIFY COLUMN`:

```sql
ALTER TABLE events
    MODIFY SUBCOLUMN payload.timestamp CODEC(ZSTD);
```

targets tuple element `timestamp` of column `payload`.

```sql
ALTER TABLE events
    MODIFY COLUMN `payload.timestamp` CODEC(ZSTD);
```

targets a physical top-level column whose literal name is `payload.timestamp`.

Using separate keywords avoids changing the interpretation of existing `ALTER TABLE ... MODIFY COLUMN` statements.

## Metadata-Only Behavior

Changing only a root or subcolumn codec is metadata-only:

- Existing parts are not immediately rewritten.
- New parts use the new codec policy.
- Later merges and mutations use the current policy when rewriting streams.
- Existing parts remain readable using the codec identifiers stored in their compressed blocks.
