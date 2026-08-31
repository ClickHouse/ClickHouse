# SQL Syntax Without `MODIFY SUBCOLUMN` or Dotted Codec Targets

## Status

This is the selected SQL model for per-tuple-element codecs.

It does not introduce `MODIFY SUBCOLUMN`, and it does not extend dotted `MODIFY COLUMN` targets to tuple-element codecs. A tuple element is changed only by modifying the declaration of its owning top-level column.

This keeps codec syntax aligned with existing type syntax: because this statement does not currently modify the type of a tuple element,

```sql
ALTER TABLE events
    MODIFY COLUMN payload.timestamp UInt64;
```

it also does not acquire a special codec-only interpretation.

## `CREATE TABLE`

The `CREATE TABLE` syntax remains local to tuple elements:

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

The annotations remain codec metadata owned by `payload`. They are not part of the `DataTypeTuple` identity.

## Where Element Annotations Are Allowed

An element `CODEC` is storage metadata, so it is accepted only while parsing a stored column declaration. The owning column must itself be a `Tuple`, and every annotated element must be reachable through a chain of `Tuple` elements. Nested tuples such as `payload.metadata.source` in the example above are supported because `metadata` is itself a direct element of `payload`.

A `Tuple` appearing only as a general data type has no owning `ColumnDescription` in which to store the policy. Element annotations are therefore rejected in casts and other expression-level type syntax, for example:

```sql
SELECT CAST((1, 'x') AS Tuple(id UInt64 CODEC(Delta, ZSTD), value String));
```

The first implementation also rejects annotations reached through a non-`Tuple` wrapper:

```sql
CREATE TABLE wrapped
(
    values Array(Tuple(id UInt64 CODEC(Delta, ZSTD), value String))
)
ENGINE = MergeTree
ORDER BY tuple();
```

Although `values` is a stored column, the annotated `Tuple` is below `Array`, not on a direct tuple-element path rooted at the owning column. Supporting such declarations later would require defining wrapper-aware logical paths and inheritance; the syntax must not be accepted and silently ignored in the meantime.

A codec can still be placed on the owning wrapper column using the existing root syntax:

```sql
values Array(Tuple(id UInt64, value String)) CODEC(ZSTD)
```

## Changing an Element Codec

Restate the owning column's tuple declaration and change the annotation on the desired element:

```sql
ALTER TABLE events
    MODIFY COLUMN payload Tuple(
        timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
        value Float64,
        metadata Tuple(
            source String,
            sequence UInt64
        )
    );
```

This changes the declaration on `payload.timestamp`. Existing codec declarations on other elements are retained when their declarations contain no codec operation.

Several element codecs can be changed together:

```sql
ALTER TABLE events
    MODIFY COLUMN payload Tuple(
        timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
        value Float64 CODEC(Gorilla, ZSTD(2)),
        metadata Tuple(
            source String CODEC(ZSTD(5)),
            sequence UInt64
        )
    );
```

Although the complete tuple type is restated, an ALTER which leaves the logical type unchanged is metadata-only. It does not schedule a mutation merely because codec annotations changed.

## Removing an Element Declaration

Omitting `CODEC` is not a remove operation. It means that the existing declaration at that path is retained. This prevents an unrelated type edit from silently deleting codec metadata.

Use an ALTER-only element modifier to remove a declaration explicitly:

```sql
ALTER TABLE events
    MODIFY COLUMN payload Tuple(
        timestamp DateTime64(3) REMOVE CODEC,
        value Float64,
        metadata Tuple(
            source String,
            sequence UInt64
        )
    );
```

`REMOVE CODEC` removes only the declaration attached directly to `timestamp`. The element then inherits the nearest ancestor declaration or uses the part default.

Nested declarations are removed in the same way:

```sql
ALTER TABLE events
    MODIFY COLUMN payload Tuple(
        timestamp DateTime64(3),
        value Float64,
        metadata Tuple(
            source String REMOVE CODEC,
            sequence UInt64
        )
    );
```

The declaration being removed must exist at that exact tuple path. An inherited effective codec does not count.

Element-level `REMOVE CODEC` is valid only in an ALTER column declaration. It is rejected in `CREATE TABLE`, persisted metadata, casts, and ordinary data-type expressions. Persisted metadata contains the resulting policy and never contains a pending remove operation.

## Root Codec Operations

Existing root syntax remains unchanged:

```sql
ALTER TABLE events
    MODIFY COLUMN payload CODEC(ZSTD(1));
```

```sql
ALTER TABLE events
    MODIFY COLUMN payload REMOVE CODEC;
```

A typed owner declaration may change both root and element declarations:

```sql
ALTER TABLE events
    MODIFY COLUMN payload Tuple(
        timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD),
        value Float64,
        metadata Tuple(
            source String REMOVE CODEC,
            sequence UInt64
        )
    ) CODEC(LZ4);
```

The root `CODEC(LZ4)` replaces the root declaration. The element annotation sets `timestamp`, the element remove operation removes `metadata.source`, and omitted element operations preserve their existing declarations.

## Explicit `Default`

`CODEC(Default)` remains different from removing a declaration:

```sql
ALTER TABLE events
    MODIFY COLUMN payload Tuple(
        timestamp DateTime64(3) CODEC(Default),
        value Float64,
        metadata Tuple(
            source String,
            sequence UInt64
        )
    );
```

This stores an explicit declaration on `timestamp`. It selects the part default and blocks inheritance from the root or another ancestor.

## Unnamed Tuples and Literal Dots

Unnamed tuple elements are edited by their position in the restated declaration:

```sql
ALTER TABLE events
    MODIFY COLUMN pair Tuple(
        UInt64 CODEC(Delta, ZSTD),
        String
    );
```

No path syntax is needed for tuple element names containing dots because the element remains local to its tuple declaration:

```sql
ALTER TABLE events
    MODIFY COLUMN payload Tuple(
        `metadata.source` String CODEC(ZSTD(5)),
        value Float64
    );
```

## Patch Semantics

The typed `MODIFY COLUMN` declaration carries a codec-policy patch:

- `CODEC(...)` on the owner sets or replaces the root declaration.
- Existing owner-level `REMOVE CODEC` removes the root declaration.
- `CODEC(...)` on a tuple element sets or replaces the declaration at that canonical path.
- `REMOVE CODEC` on a tuple element removes the declaration at that exact path.
- No codec operation on a tuple element preserves the declaration currently stored at that path.

The logical type remains a complete replacement declaration, following existing `MODIFY COLUMN` behavior. Codec operations are patches because silently removing omitted codec metadata would make ordinary type maintenance unsafe.

Apply all element operations to a clone of the existing policy, then validate the complete resulting policy against the resulting logical type. If the type change removes a path with a retained declaration, the ALTER fails unless that declaration is explicitly removed in the same statement.

## Parser and AST Contract

The parser needs two distinct tuple-element annotation states in ALTER declarations:

```text
no operation
set CODEC(...)
remove CODEC
```

The ordinary codec AST is sufficient for the set state. The remove state must be represented separately; it must not be encoded as `CODEC(Default)` or as a null codec AST.

The parser mode is contextual:

- `CREATE TABLE` accepts an optional element `CODEC(...)`.
- Typed `ALTER TABLE ... MODIFY COLUMN` accepts an optional element `CODEC(...)` or `REMOVE CODEC`.
- Generic `ParserDataType` users accept neither storage annotation.
- Persisted metadata accepts concrete `CODEC(...)` annotations but never `REMOVE CODEC`.

## Name Resolution

`MODIFY COLUMN` continues to resolve exactly as it does today. The target is always the owning column name:

```sql
MODIFY COLUMN payload Tuple(...)
```

No schema-dependent reinterpretation of `payload.timestamp` is introduced. Existing behavior for flattened `Nested` columns and physical column names containing dots is therefore unchanged.

## Tradeoffs

Advantages:

- No new ALTER command.
- No new meaning for dotted `MODIFY COLUMN` targets.
- `CREATE TABLE` and ALTER use the same local tuple-element layout.
- Literal-dot and positional element names require no ALTER path grammar.
- One statement can update several element codecs atomically.

Costs:

- The full owning tuple type must be restated for a small codec change.
- Tooling must preserve the complete logical type accurately.
- Explicit element-level `REMOVE CODEC` adds an ALTER-only state to the tuple declaration grammar.
- A typed ALTER must distinguish logical-type replacement from codec-policy patch semantics.

## Assessment

This model is adopted because it is the most conservative extension of existing `MODIFY COLUMN` target resolution. It avoids suggesting that tuple elements are independently modifiable columns.

Its main cost is verbosity. Codec changes are schema operations, and `SHOW CREATE TABLE` provides the complete normalized owner declaration that can be edited and submitted.
