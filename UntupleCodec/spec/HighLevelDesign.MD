# High-Level Design for Per-Subcolumn Codecs

## Goal

Allow different compression codecs to be declared for elements of a `Tuple` column while preserving the existing behavior of top-level column codecs.

Codec policy remains table metadata. It must not become part of `IColumn`, the identity of `IDataType`, or a permanently configured `ISerialization` object.

Exact SQL parsing is outside the scope of this document. It is assumed that `CREATE TABLE` and `ALTER TABLE` can provide a root codec and zero or more subcolumn codec declarations.

## Codec Metadata Model

Generalize the single `ColumnDescription::codec` value into a codec-policy abstraction, tentatively called `ColumnCodecDescription`.

The policy contains:

- An optional codec for the top-level column.
- Optional codecs keyed by canonical logical subcolumn paths.
- Operations for cloning, equality, formatting, serialization, and effective-codec resolution.

Paths must be logical paths made of tuple-element names or positions. They must not use physical filenames because filenames and stream layouts are storage-format details.

The effective codec for a logical path is resolved using these rules:

1. The most specific declaration wins.
2. A subcolumn without a declaration inherits the nearest ancestor declaration.
3. If no ancestor has a declaration, the stream uses the resolved part default.
4. Explicit `CODEC(Default)` overrides an ancestor declaration and selects the part default.
5. A declaration on a tuple element applies to all descendants of that element unless a deeper declaration overrides it.

Existing metadata with a single codec is interpreted as a policy containing only a root declaration. Its behavior must remain unchanged.

## Metadata Lifecycle

Populate the codec policy when interpreting `CREATE TABLE` and when parsing an `AlterCommand`.

Update all `ColumnDescription` operations to preserve the complete policy:

- Copy and move operations.
- Equality checks.
- Normalized `CREATE TABLE` formatting.
- Text serialization and deserialization.
- Replicated metadata.
- Metadata reload and attach.
- Backup and restore.

`ALTER` must support adding, replacing, and removing an individual subcolumn declaration. A codec-only change remains metadata-only and does not immediately rewrite existing parts.

Validation must reject:

- Paths that do not resolve to a tuple element.
- Duplicate declarations for the same logical path.
- Codec declarations incompatible with the selected element type.
- Ambiguous or conflicting codec choices for one shared physical stream.

## Codec Validation

Generalize codec validation to accept a codec policy and a column type.

- Validate a root codec against all applicable descendant leaf streams, preserving current behavior.
- Validate a subcolumn codec only against that subcolumn's type and descendant streams.
- Instantiate type-dependent codecs with the leaf type of every applicable stream.
- Preserve generic-only handling for structural streams such as array offsets and null maps.
- Preserve current checks for experimental, suspicious, encrypting, and lossy codecs.

The policy should be validated and normalized before it is stored in `ColumnDescription`, so writers consume trusted metadata.

Codec-driven serialization customization must also become path-aware. In particular, the logic that attaches `Quantized` serialization currently inspects only the root `ColumnDescription::codec`. It must locate a `Quantized` declaration on a tuple element and attach the customization to that exact type subtree without modifying unrelated elements or shared type instances.

## Writer Integration

Change the MergeTree writer interface from passing one codec AST to passing a codec resolver or the full `ColumnCodecDescription`.

Current interface:

```cpp
addStreams(const NameAndTypePair & column, const ASTPtr & effective_codec_desc);
```

Conceptual new interface:

```cpp
addStreams(const NameAndTypePair & column, const ColumnCodecResolver & codec_resolver);
```

For every callback from `ISerialization::enumerateStreams`, the writer should:

1. Derive the logical tuple path from `ISerialization::SubstreamPath`.
2. Resolve the most specific codec declaration for that path.
3. Resolve `Default` using the part's effective default codec.
4. Instantiate the codec with the stream's leaf subtype.
5. Apply generic-only filtering to structural streams.
6. Decide adaptive-codec eligibility for this individual stream.
7. Create or reuse the compressed stream as appropriate for the part format.

Apply the same resolution in:

- `MergeTreeDataPartWriterWide`.
- `MergeTreeDataPartWriterCompact`.
- Full-column writers.
- Vertical-merge and partial-column writers.

Partial writers must combine `NameAndTypePair::getNameInStorage` with the serialization path so that resolution always starts from the owning top-level `ColumnDescription`.

If multiple logical paths resolve to the same shared physical stream, their effective generic codec must agree. Reject incompatible declarations instead of selecting whichever path is enumerated first.

## Defaults and Recompression

Keep existing precedence between explicit column codecs and the part default:

- An explicit root or subcolumn codec overrides the part default.
- An undeclared subcolumn uses its nearest declared ancestor or the part default.
- Explicit `Default` uses the part default even when an ancestor has an explicit codec.
- A `RECOMPRESS` TTL changes the part default but does not override explicit root or subcolumn declarations.

A codec-only metadata change does not rewrite existing parts. New parts and parts rewritten by later merges or mutations use the current policy. Readers continue to identify the actual codec from each compressed block.

## Other Storage Engines

Audit every writer that consumes `ColumnDescription::codec`.

`StorageLog` currently resolves one codec for a top-level column before creating its streams. It should use the same logical-path resolver if its physical layout can support different stream codecs. An engine that cannot represent the policy must reject it explicitly rather than silently applying the root codec.

## Introspection

Update user-visible metadata consistently:

- `SHOW CREATE TABLE` round-trips the complete policy.
- `DESCRIBE TABLE` shows the root declaration for the top-level column.
- `DESCRIBE TABLE` with subcolumns enabled shows each subcolumn's effective codec.
- `system.columns.codec_expression` returns the normalized complete policy.
- Physical codec introspection continues reporting codecs found in data blocks.

## Compatibility

The change must preserve:

- Parsing and loading of existing single-codec metadata.
- Existing top-level `CODEC` semantics.
- Reading old parts after metadata changes.
- Replication between servers that understand the new syntax and metadata representation.

Compatibility with older server versions requires a feature/version gate if the persisted metadata syntax cannot be parsed by them. A table using only a root codec should continue to be serialized in the existing form.

## Test Plan

Add functional tests for:

- Named and positional tuples with different element codecs.
- A root codec inherited by elements.
- An element codec overriding its parent.
- Explicit `Default` overriding a parent codec.
- Nested tuples and nested overrides.
- Mixed element types, such as `Delta` for integers and `ZSTD` for strings.
- `Nullable`, `Array`, and other multi-stream element types.
- Structural-stream generic codec filtering.
- `Quantized` and other codec-driven serialization customization.
- Wide and compact parts.
- Inserts, reads, merges, mutations, restart, attach, backup, and restore.
- Replicated metadata round-trips.
- Codec-only `ALTER` affecting future writes without immediately rewriting old parts.
- Removing one subcolumn codec without changing sibling declarations.
- Invalid paths, incompatible codecs, duplicates, and shared-stream conflicts.
- Existing tables with one root codec retaining their current behavior.

Use physical codec inspection to verify that each tuple element's stream was actually written with the expected codec, rather than relying only on `SHOW CREATE TABLE` output.
