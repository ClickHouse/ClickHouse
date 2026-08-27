# Experimental SQL Without `MODIFY SUBCOLUMN`

## Status

This is an alternative to the `ALTER TABLE` syntax proposed in `SQL.MD`. It uses the existing `MODIFY COLUMN` form for both top-level columns and tuple elements. It does not require a new `MODIFY SUBCOLUMN` command.

The storage model does not require `MODIFY SUBCOLUMN`. Internally, both forms modify the codec policy owned by one top-level `ColumnDescription`. The only additional information needed by `MODIFY COLUMN` is an optional, segmented tuple path.

## `CREATE TABLE`

The `CREATE TABLE` syntax is unchanged from `SQL.MD`:

```sql
CREATE TABLE events
(
    id UInt64,
    payload Tuple(
        timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
        value Float64 CODEC(Gorilla, ZSTD(1)),
        metadata Tuple(
            source String CODEC(ZSTD(3)),
            sequence UInt64
        )
    ) CODEC(LZ4)
)
ENGINE = MergeTree
ORDER BY id;
```

Tuple-element annotations remain storage metadata and are not part of the `DataTypeTuple` identity.

## Changing a Codec

The existing top-level form remains unchanged:

```sql
ALTER TABLE events
    MODIFY COLUMN payload CODEC(ZSTD(1));
```

A compound column path changes the declaration on a tuple element:

```sql
ALTER TABLE events
    MODIFY COLUMN payload.timestamp
    CODEC(DoubleDelta, ZSTD(1));
```

Nested tuple elements use additional path segments:

```sql
ALTER TABLE events
    MODIFY COLUMN payload.metadata.source
    CODEC(ZSTD(5));
```

Multiple declarations can be changed in one query:

```sql
ALTER TABLE events
    MODIFY COLUMN payload.timestamp CODEC(DoubleDelta, ZSTD(1)),
    MODIFY COLUMN payload.value CODEC(Gorilla, ZSTD(1)),
    MODIFY COLUMN payload.metadata.source CODEC(ZSTD(5));
```

This operation changes only codec metadata. It does not modify the tuple element's logical type.

## Removing a Declaration

The same `REMOVE CODEC` modifier applies at either level:

```sql
ALTER TABLE events
    MODIFY COLUMN payload.timestamp REMOVE CODEC;
```

This removes only the declaration attached to `timestamp`. It does not remove an inherited codec. After removal, the element inherits the nearest ancestor declaration or uses the part default.

The existing root operation remains unchanged:

```sql
ALTER TABLE events
    MODIFY COLUMN payload REMOVE CODEC;
```

## Explicit `Default`

An element can stop inheritance and select the part default:

```sql
ALTER TABLE events
    MODIFY COLUMN payload.timestamp CODEC(Default);
```

`CODEC(Default)` is an explicit declaration. It must not be treated as if the declaration were absent.

## Path Syntax

The parser must retain identifier segments instead of flattening the formatted name into one string.

An unquoted dot separates path segments:

```sql
MODIFY COLUMN payload.metadata.source CODEC(ZSTD)
```

This produces the segmented path `payload`, `metadata`, `source`.

A quoted segment may contain a literal dot:

```sql
MODIFY COLUMN payload.`metadata.source` CODEC(ZSTD)
```

This produces the path `payload`, `metadata.source`.

Positional elements of an unnamed tuple use quoted numeric segments:

```sql
ALTER TABLE events
    MODIFY COLUMN pair.`1` CODEC(Delta, ZSTD);
```

Quoting the complete name produces one identifier segment and therefore addresses a physical top-level column whose literal name contains a dot:

```sql
ALTER TABLE events
    MODIFY COLUMN `payload.timestamp` CODEC(ZSTD);
```

## Name Resolution

For a codec-only `MODIFY COLUMN`, resolve the target as follows:

1. A syntactically single-segment identifier targets a top-level column. This includes a fully quoted name containing dots.
2. For a compound identifier, look up its first segment as the owning top-level column and resolve the remaining segments through its logical `Tuple` type.
3. If the first segment is not an owning tuple column, retain the existing compound-column lookup. This preserves operations on flattened `Nested` columns and other existing physical dotted names.
4. If both interpretations are valid, the tuple-path interpretation wins. The physical dotted column remains addressable by quoting its complete name.
5. Every path segment must select an existing tuple element. Reaching a non-`Tuple` type before consuming the path is an error.

Resolution is based on parsed segments, not by splitting a formatted identifier on dots.

## Restricted Modifier Set

When a compound target resolves to a tuple element, initially allow only:

- `CODEC(...)`
- `REMOVE CODEC`

For example, this is rejected:

```sql
ALTER TABLE events
    MODIFY COLUMN payload.timestamp UInt64;
```

Changing a tuple element type still requires modifying the owning column's complete type:

```sql
ALTER TABLE events
    MODIFY COLUMN payload Tuple(
        timestamp UInt64,
        value Float64
    );
```

Likewise, element-level `DEFAULT`, `TTL`, `COMMENT`, and `SETTINGS` are outside this proposal. Restricting the modifier set avoids implying that tuple elements have independent `ColumnDescription` entries.

## Metadata Semantics

Both root and element codec changes are metadata-only:

- Existing parts are not rewritten immediately.
- New parts use the updated policy.
- Merges and mutations use the current policy when rewriting streams.
- Old parts remain readable because compressed blocks record their codec identifiers.

The internal operation contains:

- the owning top-level column name;
- zero or more canonical tuple path segments;
- either a codec AST or a remove action.

An empty path modifies the root declaration. A non-empty path modifies exactly that tuple-element declaration. The implementation can therefore lower both SQL forms to the existing `AlterCommand::MODIFY_COLUMN` machinery.

## Compatibility Tradeoff

The syntax is implementable, but it slightly extends the meaning of an unquoted compound target in `MODIFY COLUMN`. A statement such as:

```sql
MODIFY COLUMN payload.timestamp CODEC(ZSTD)
```

previously could only resolve through existing compound-column behavior. With this proposal, it targets a tuple element when `payload` is a tuple column containing `timestamp`.

The interpretation is deterministic, and a literal top-level column named `payload.timestamp` remains accessible as `` `payload.timestamp` ``. Nevertheless, this is a compatibility consideration that the explicit `MODIFY SUBCOLUMN` syntax avoids.

## Assessment

This proposal is technically feasible and produces a smaller SQL surface:

- `MODIFY COLUMN` changes codec metadata for both a column and a path within that column.
- The presence of a compound path determines whether the root or an element declaration is changed.
- The implementation already represents the operation as an owning column plus an optional codec path, so no distinct storage command is necessary.

The decision between this syntax and `MODIFY SUBCOLUMN` is therefore a language-design and compatibility choice, not a storage limitation.
