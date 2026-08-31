# Stateless SQL Test Plan for Per-Tuple-Element Codecs

## Purpose

This plan verifies the SQL model selected in `SQLNoModifySubcolumnNoDots.MD` from parsing through persisted metadata and physical `MergeTree` streams. The tests must prove both the declared policy and the codecs actually written; successful reads alone are insufficient because compressed blocks are self-describing and could hide a writer-side policy error.

The six tests were allocated with `tests/queries/0_stateless/add-test` and have the filenames below.

The initial suite consists of these files:

| Allocated file | Primary purpose |
|---|---|
| `05028_tuple_element_codecs_metadata.sql` | `CREATE TABLE` syntax, normalization, introspection, and metadata reload |
| `05028_tuple_element_codecs_wide.sql` | Effective policy and physical codecs in wide parts |
| `05028_tuple_element_codecs_alter.sql` | Typed `MODIFY COLUMN` patch semantics and old/new parts |
| `05028_tuple_element_codecs_invalid.sql` | Non-column tuples, unsupported wrappers, and invalid element operations |
| `05028_tuple_element_codecs_structural.sql` | Array and nullable structural streams and generic-only filtering |
| `05028_tuple_element_codecs_compact.sql` | Compact-part stream construction, reads, merges, and mutations |

All tests use unique table names, begin with `DROP TABLE IF EXISTS` cleanup, and drop successfully created tables at the end. Output queries select only stable fields and use an explicit `ORDER BY` wherever row order is relevant.

## Common Physical-Codec Assertion

For wide parts, use `mergeTreeCodecBlockCounts` and normalize its output to the fields relevant to this feature:

```sql
SELECT
    part_name,
    substream,
    mapKeys(codec_block_counts) AS codecs,
    arrayMin(mapValues(codec_block_counts)) > 0 AS all_positive
FROM mergeTreeCodecBlockCounts(currentDatabase(), table_name)
WHERE column = 'payload'
ORDER BY part_name, substream;
```

Set `min_bytes_for_wide_part = 0`, stop background merges when part identity matters, and insert enough rows to create non-empty codec blocks. The `no-random-merge-tree-settings` tag is justified for tests that inspect per-stream files because randomized part-format or block-number settings can remove or add introspection rows.

Compact parts have no per-stream `.bin` file, so `mergeTreeCodecBlockCounts` cannot attribute codec block headers to individual columns. The compact test therefore verifies compact part creation, correct reads, successful merge/mutation rewrites, and empty per-stream codec maps. Wide-part tests remain the authoritative physical-codec assertions.

## `05028_tuple_element_codecs_metadata.sql`

### Table definitions

Create one named and one unnamed tuple column:

```sql
CREATE TABLE t_tuple_codec_metadata
(
    key UInt64,
    payload Tuple(
        id UInt64 CODEC(Delta, ZSTD(1)),
        text String CODEC(ZSTD(3)),
        nested Tuple(
            score Float64 CODEC(Gorilla, ZSTD(1)),
            label String
        ),
        defaulted UInt32 CODEC(Default),
        `literal.dot` String CODEC(LZ4HC(4))
    ) CODEC(LZ4),
    pair Tuple(
        UInt64 CODEC(T64, LZ4),
        String CODEC(ZSTD(2))
    )
)
ENGINE = MergeTree
ORDER BY key;
```

This single schema covers root inheritance, direct overrides, a nested direct-`Tuple` path, explicit `Default`, a literal dot in an element name, and positional element paths.

### Assertions

1. Inspect `system.tables.create_table_query` and assert the canonical declaration has the expected annotation count and contains representative literal-dot and positional annotations locally in the tuple declaration. The logical type text must not gain a second metadata-only wrapper or dotted target syntax.
2. Query stable properties of `system.columns.type` and `system.columns.compression_codec`, ordered by `position`.
   - `type` must be the undecorated logical type.
   - `compression_codec` for `payload` and `pair` must contain the complete normalized policy.
   - `key` remains unchanged.
3. Run `DESCRIBE TABLE t_tuple_codec_metadata` in `JSONEachRow` format.
   - The top-level `payload` row reports only root `LZ4`.
   - The top-level `pair` row has no root expression.
4. Run `DESCRIBE TABLE t_tuple_codec_metadata SETTINGS describe_include_subcolumns = 1` in `JSONEachRow` format and record its deterministic rows, including the element and String `.size` subcolumns.
   - Each subcolumn row reports its effective declaration.
   - `payload.nested.label` inherits `LZ4`.
   - `payload.defaulted` reports `Default`, not the current server codec substituted for it.
   - Literal-dot and nested paths remain distinct.
5. Query `toTypeName(payload)` and `toTypeName(pair)` after inserting one row. The result must contain ordinary `Tuple` types without `CODEC` text.
6. Execute `DETACH TABLE ...` followed by `ATTACH TABLE ...`, then repeat the stable `create_table_query` assertions. This exercises `ColumnDescription::writeText`, `ColumnDescription::readText`, and metadata reconstruction.
7. Rename the table, then rename it back, and repeat the `system.columns` declaration-count query. This catches policy loss through metadata cloning.

Do not use physical codec counts in this file; its responsibility is deterministic metadata and AST round-tripping.

## `05028_tuple_element_codecs_wide.sql`

### Table definition

```sql
CREATE TABLE t_tuple_codec_wide
(
    key UInt64,
    payload Tuple(
        transformed UInt64 CODEC(Delta, LZ4),
        inherited String,
        nested Tuple(
            transformed UInt32 CODEC(T64, LZ4),
            inherited String
        ),
        defaulted UInt64 CODEC(Default)
    ) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS
    min_bytes_for_wide_part = 0,
    min_compress_block_size = 0,
    max_compress_block_size = 65536,
    default_compression_codec = 'LZ4';
```

Insert at least `100000` deterministic rows. Use varying integer values and repeated strings so every leaf produces data while reference output remains independent of compression ratio.

### Expected effective policy

| Substream | Expected codec key |
|---|---|
| `payload.transformed` | `Delta(8), LZ4` |
| `payload.inherited` | `ZSTD(1)` |
| `payload.nested.transformed` | `T64, LZ4` |
| `payload.nested.inherited` | `ZSTD(1)` |
| `payload.defaulted` | `LZ4` |

Use the actual escaped `substream` names returned by `mergeTreeCodecBlockCounts` in the reference file. Assert `all_positive = 1` for every row. Then read the whole tuple and each element separately and compare aggregate checksums, proving that full-column and subcolumn reads agree.

Run `OPTIMIZE TABLE ... FINAL` after a second insert and repeat the physical assertion. The merged part must use the same policy. This covers ordinary horizontal merge output.

## `05028_tuple_element_codecs_alter.sql`

### Initial table and first part

```sql
CREATE TABLE t_tuple_codec_alter
(
    key UInt64,
    payload Tuple(
        kept UInt64 CODEC(ZSTD(1)),
        changed String,
        nested Tuple(
            removed UInt64 CODEC(T64, LZ4),
            defaulted UInt64
        )
    ) CODEC(LZ4)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, default_compression_codec = 'ZSTD(2)';
```

Stop merges and insert the first deterministic block. After applying the combined patch and inserting the second block, record physical codec counts for both parts.

### Combined patch

Apply one typed owner alteration:

```sql
ALTER TABLE t_tuple_codec_alter
    MODIFY COLUMN payload Tuple(
        kept UInt64,
        changed String CODEC(ZSTD(3)),
        nested Tuple(
            removed UInt64 REMOVE CODEC,
            defaulted UInt64 CODEC(Default)
        )
    ) CODEC(LZ4HC(4));
```

Assert through `system.columns.compression_codec` that:

- `kept` still has `CODEC(ZSTD(1))` although its operation was omitted;
- `changed` now has `CODEC(ZSTD(3))`;
- `nested.removed` has no direct declaration and therefore inherits the new root;
- `nested.defaulted` stores explicit `CODEC(Default)`;
- the root is `CODEC(LZ4HC(4))`.

Insert the second deterministic block. Query physical codecs ordered by `part_name, substream`. The first part must retain the old codecs; the second part must use the patched policy. This proves that a codec-only ALTER is metadata-only.

### Further operations

1. Run another typed `MODIFY COLUMN` which restates the identical logical type while omitting all codec operations. Existing declarations must survive, proving that omission means preserve rather than remove.
2. Remove a literal declaration once and verify that the exact declaration disappears. Attempt the same `REMOVE CODEC` again and expect `BAD_ARGUMENTS`; inherited policy is not a removable declaration. The final physical assertion verifies the resulting effective policy.
3. Attempt a type change which removes an element carrying an omitted retained declaration and expect `BAD_ARGUMENTS` rather than silently leaving a dangling path.
4. Exercise a positional tuple rename combined with `REMOVE CODEC`; the removal must identify the old declaration by position rather than accidentally leaving it under the old name.
5. Exercise the existing root-only forms `MODIFY COLUMN payload CODEC(...)` and `MODIFY COLUMN payload REMOVE CODEC`. Neither may alter sibling element declarations.
6. Restart merges, run `OPTIMIZE TABLE ... FINAL`, and verify that the resulting part uses the current policy for every stream.

Every failure uses the symbolic server error annotation supported by stateless tests. Prefer the most specific stable error code exposed by the implementation; do not pin full exception text.

## `05028_tuple_element_codecs_invalid.sql`

This file fixes the boundary between storage column declarations and ordinary `Tuple` types.

### A `Tuple` that is not a stored column declaration

The following contexts have no owning `ColumnDescription`; an element codec must be rejected by the parser instead of becoming part of `IDataType`:

```sql
SELECT CAST((1, 'x') AS Tuple(id UInt64 CODEC(Delta, ZSTD), value String));
SELECT CAST((1, 'x'), 'Tuple(id UInt64 CODEC(Delta, ZSTD), value String)');
```

Also include a control cast without element codecs and assert the ordinary tuple type. This demonstrates that the rejection is specifically about storage metadata, not tuples themselves.

A tuple produced by an expression is valid data but has no element codec declaration:

```sql
SELECT toTypeName(tuple(toUInt64(1), 'x'));
```

There is no expression-level syntax which attaches a storage codec to this value. A `CREATE TABLE ... AS SELECT tuple(...)` control case creates an ordinary tuple column, and `system.tables.create_table_query` must contain no element codec until the owning column is explicitly declared or modified with the selected column-declaration syntax.

### A `Tuple` below a non-`Tuple` wrapper

The first implementation supports only a chain of tuple elements rooted at the owning `Tuple` column. Reject these declarations with `BAD_ARGUMENTS` rather than silently dropping the annotation:

```sql
CREATE TABLE t_tuple_codec_wrapped_array
(
    value Array(Tuple(id UInt64 CODEC(Delta, ZSTD), text String))
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE t_tuple_codec_wrapped_map
(
    value Map(String, Tuple(id UInt64 CODEC(Delta, ZSTD), text String))
)
ENGINE = MergeTree
ORDER BY tuple();
```

Add controls showing that a root codec on each wrapper column remains legal, for example `Array(Tuple(id UInt64, text String)) CODEC(ZSTD)`.

### Invalid element operations

Cover:

- element `REMOVE CODEC` in `CREATE TABLE`;
- element `REMOVE CODEC` in `ADD COLUMN`;
- `REMOVE CODEC` where no declaration exists at that exact element;
- tuple-element `CODEC(Quantized(...))`, currently rejected with `NOT_IMPLEMENTED` until codec-driven serialization is path-aware;
- element annotations below `Array` and `Map` wrappers in typed `MODIFY COLUMN`;
- a codec on an `ALIAS` tuple column;
- inserting into a `Log` table with a per-element policy, which currently rejects per-subcolumn codecs explicitly rather than applying only the root.

Use one successful direct nested-`Tuple` declaration as a control, proving that the wrapper rejection does not accidentally reject `Tuple(... nested Tuple(... CODEC(...)))`.

## `05028_tuple_element_codecs_structural.sql`

### Table definition

```sql
CREATE TABLE t_tuple_codec_structural
(
    key UInt64,
    payload Tuple(
        array_value Array(UInt64) CODEC(Delta, ZSTD(1)),
        nullable_value Nullable(UInt64) CODEC(Delta, LZ4),
        plain_array Array(String) CODEC(ZSTD(3))
    )
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0;
```

Insert arrays of varying lengths, alternating nulls, and non-empty strings.

### Assertions

Use `mergeTreeCodecBlockCounts` to list every `payload` substream.

- Value streams below `array_value` use `Delta(8), ZSTD(1)`.
- Array-size streams below `array_value` use only the generic `ZSTD(1)` component.
- The value stream below `nullable_value` uses `Delta(8), LZ4`.
- Its null-map stream uses only `LZ4`.
- Both the sizes and values below `plain_array` use `ZSTD(3)`.

Read deterministic aggregate properties through individual tuple elements, including array offsets through `length` and nullable values through `isNull`. After `OPTIMIZE TABLE ... FINAL`, repeat the physical assertion and stable row/null counts.

If exact substream naming differs by part-format settings, use the names emitted by the existing `mergeTreeCodecBlockCounts` tests and order them explicitly. Do not weaken the test to a table-wide set of codecs; it must associate each codec with the correct physical substream.

## `05028_tuple_element_codecs_compact.sql`

### Table definition

Create the same logical policy as the wide test but force compact parts:

```sql
CREATE TABLE t_tuple_codec_compact
(
    key UInt64,
    payload Tuple(
        number UInt64 CODEC(Delta, LZ4),
        text String CODEC(ZSTD(1)),
        inherited UInt64
    ) CODEC(LZ4HC(4))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000;
```

### Assertions

1. Insert several parts and assert `system.parts.part_type = 'Compact'` for active parts.
2. Verify deterministic element-level aggregate queries against expected values.
3. Query `mergeTreeCodecBlockCounts`; rows may describe the logical substreams, but `codec_block_counts` must be empty and byte sizes null as in the existing compact test.
4. Run `OPTIMIZE TABLE ... FINAL`, confirm the merged part remains compact, and repeat the empty-map and null-size assertions.
5. Run a mutation which assigns a reconstructed value to the complete `payload` column and wait synchronously. Repeat reads and part-type assertions. This exercises the mutation writer with a per-element policy even though physical block attribution is unavailable.
6. Repeat stable `system.tables.create_table_query` fragment and declaration-count assertions after merge and mutation to prove writer operations did not alter metadata.

## Follow-up Coverage Outside the Initial Stateless Set

The following remain required by the broader low-level design but should be added when their implementation is present and stable:

- vertical merges forced by deterministic merge settings, including codec agreement for shared offset streams;
- `RECOMPRESS` TTL with explicit siblings and `CODEC(Default)` siblings;
- adaptive default selection per tuple element;
- replicated database metadata synchronization and mixed-version feature gating;
- backup and restore;
- path-aware `Quantized` serialization after the current `NOT_IMPLEMENTED` restriction is removed;
- shared streams across flattened `Nested`, `Dynamic`, and `JSON` layouts;
- storage engines other than `MergeTree` once they support the resolver rather than rejecting the policy.

These follow-ups must get separate descriptive stateless files instead of expanding the six core files indefinitely.

## Completion Criteria

The initial SQL suite is complete when:

- all selected `CREATE TABLE` and typed owner `MODIFY COLUMN` forms round-trip through metadata;
- both named and positional elements, nested tuple paths, literal-dot names, inheritance, overrides, removal, omission, and explicit `Default` are covered;
- physical wide-part assertions prove different tuple elements receive different codecs;
- compact, merge, and mutation paths read correctly;
- ordinary expression-level tuples and wrapped tuples reject element annotations explicitly;
- existing root-only codec tests pass without reference changes.
