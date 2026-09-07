# High-Level Design for Per-Subcolumn Codecs

## Goal

Allow different compression codecs to be declared for elements of a `Tuple` column while preserving the existing behavior of top-level column codecs.

Codec policy remains table metadata. It must not become part of `IColumn`, the identity of `IDataType`, or a permanently configured `ISerialization` object.

The selected SQL model is specified in `SQLNoModifySubcolumnNoDots.MD`. Element declarations are local modifiers inside a stored column's `Tuple` declaration. An `ALTER TABLE ... MODIFY COLUMN` restates the owning top-level column type and carries set, remove, or preserve operations for its tuple elements. There is no `MODIFY SUBCOLUMN` command and no dotted codec target.

An owning column may be a `Tuple`, `Array(Tuple(...))`, or `SimpleAggregateFunction(f, Array(Tuple(...)))`. `Array` and `SimpleAggregateFunction` are transparent when deriving codec paths: only Tuple element names add path segments. A `Tuple` used only in an expression has no storage codec policy. An annotation reached through `Map`, `Nullable`, `LowCardinality`, `Nested`, `JSON`, or another unsupported wrapper is rejected rather than ignored.

In the parser AST, the codec operation belongs to the owning `ASTTupleDataType`. An annotated Tuple has a sparse list of operations keyed by zero-based element position. Each operation is tagged as `Set` or `Remove`; a `Set` owns its `CODEC` AST as an ordinary child. This keeps `ASTDataType` and specialized element types such as `ASTEnumDataType` free of storage policy while allowing ordinary AST visitors to reach codec expressions. Operations inside transparent wrappers remain owned by the nearest Tuple node.

## Codec Metadata Model

Generalize the single `ColumnDescription::codec` value into a codec-policy abstraction called `ColumnCodecDescription`.

The policy contains:

- An optional codec for the top-level column.
- Optional codecs keyed by canonical logical subcolumn paths.
- Operations for cloning, equality, mutation while constructing metadata, and pure longest-prefix lookup.

Paths must be logical paths made of tuple-element names or positions. They must not use physical filenames because filenames and stream layouts are storage-format details.

The effective codec for a logical path is resolved using these rules:

1. The most specific declaration wins.
2. A subcolumn without a declaration inherits the nearest ancestor declaration.
3. If no ancestor has a declaration, the stream uses the resolved part default.
4. Explicit `CODEC(Default)` overrides an ancestor declaration and selects the part default.
5. A declaration on a tuple element applies to all descendants of that element unless a deeper declaration overrides it.

Existing metadata with a single codec is interpreted as a policy containing only a root declaration. Its behavior must remain unchanged.

## Metadata Lifecycle

Populate the codec policy when interpreting `CREATE TABLE` and when parsing an `AlterCommand`. A typed `MODIFY COLUMN` is a complete logical-type declaration but a codec-policy patch: an element `CODEC(...)` sets that path, an element `REMOVE CODEC` removes that path, and an omitted element codec operation preserves the existing declaration.

Update all `ColumnDescription` operations to preserve the complete policy:

- Copy and move operations.
- Equality checks.
- Normalized `CREATE TABLE` formatting.
- Text serialization and deserialization.
- Replicated metadata.
- Metadata reload and attach.
- Backup and restore.

`ALTER` must support adding, replacing, and removing individual tuple-element declarations from the restated owner declaration. A codec-only change remains metadata-only and does not immediately rewrite existing parts.

The feature is disabled by default. `enable_tuple_element_codecs` gates statements that add or change element declarations. Existing metadata can still be attached, restored, read, preserved, and have declarations removed while the setting is disabled.

Validation must reject:

- Paths that do not resolve to a tuple element.
- Duplicate declarations for the same logical path.
- Codec declarations incompatible with the selected element type.

## Codec Validation

Generalize codec validation to accept a codec policy and a column type. Validation enumerates the real serialization streams and uses the same value-versus-structural classification as runtime resolution.

- Validate a root codec against all applicable descendant value and structural streams, preserving current behavior.
- Validate a subcolumn codec only against the streams selected by that subcolumn's path and inheritance.
- Instantiate type-dependent codecs with the leaf type of every applicable stream.
- Preserve generic-only handling for structural streams such as array offsets and null maps.
- Preserve current checks for experimental, suspicious, encrypting, and lossy codecs.

The policy should be validated and normalized before it is stored in `ColumnDescription`, so writers consume trusted metadata.

Codec-driven serialization customization is not path-aware yet. `Quantized` is therefore rejected on Tuple elements; supporting it is a separate feature that must rebuild the affected type subtree without mutating shared type instances.

## Writer Integration

Change the MergeTree writer interface from passing one codec AST to passing the full `ColumnCodecDescription`. Each writer constructs one `ColumnCodecResolver` per written column.

Current interface:

```cpp
addStreams(const NameAndTypePair & column, const ASTPtr & effective_codec_desc);
```

New interface:

```cpp
addStreams(const NameAndTypePair & column, const ColumnCodecDescription & codec_policy);
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

If multiple logical paths resolve to the same physical stream, writers preserve the existing MergeTree rule: the first writer that creates the stream owns its codec and later callbacks reuse it without resolving another codec. Flattened `Nested` siblings are the important compatibility case: they may legally have different column codecs while sharing offsets, so deterministic physical-column order selects the codec for those offsets. Ordinary value streams still receive independent codecs because their physical stream names are distinct.

## Defaults and Recompression

Keep existing precedence between explicit column codecs and the part default:

- An explicit root or subcolumn codec overrides the part default.
- An undeclared subcolumn uses its nearest declared ancestor or the part default.
- Explicit `Default` uses the part default even when an ancestor has an explicit codec.
- A `RECOMPRESS` TTL changes the part default but does not override explicit root or subcolumn declarations.

A codec-only metadata change does not rewrite existing parts. New parts and parts rewritten by later merges or mutations use the current policy. Readers continue to identify the actual codec from each compressed block.

## Other Storage Engines

Audit every writer that consumes `ColumnDescription::codec`.

Storage support is explicit through `IStorage::supportsPerSubcolumnCodecs()`, which is false by default. The MergeTree family opts in. Other engines, including `Log`, reject element policies during `CREATE` and `ALTER` until their writers use the shared resolver.

## Introspection

Update user-visible metadata consistently:

- `SHOW CREATE TABLE` round-trips the complete policy.
- `DESCRIBE TABLE` shows the root declaration for the top-level column.
- `DESCRIBE TABLE` with subcolumns enabled shows each subcolumn's effective codec.
- `system.columns.compression_codec` keeps its existing root-only meaning.
- Physical codec introspection continues reporting codecs found in data blocks.

`SHOW CREATE TABLE` is the initial full-policy representation. A structured system-table surface can be added later after its public schema is agreed.

## Compatibility

The change must preserve:

- Parsing and loading of existing single-codec metadata.
- Existing top-level `CODEC` semantics.
- Reading old parts after metadata changes.
- Writing existing flattened `Nested` schemas whose sibling Arrays have different codecs but share an offsets stream.
- Deterministic replication metadata between servers that understand metadata format version 2.

The logical type field remains undecorated. Root-only policies continue to use columns metadata version 1 with the existing byte representation. A policy with element entries uses version 2 and stores counted path segments plus codec AST text in a separate field. Version-1 metadata from an earlier implementation that embedded Tuple annotations in the type is accepted and rewritten as version 2.

There is no replica capability negotiation in this experimental version. Enabling `enable_tuple_element_codecs` is the administrator's opt-in to the rolling-upgrade and downgrade restriction: old servers cannot consume version-2 metadata. ATTACH on a capable server does not require the setting because the setting controls admission of new declarations, not loading existing metadata.

## Test Plan

The executable test matrix, concrete table definitions, queries, and planned stateless test filenames are specified in `SQLTestPlan.MD`.
