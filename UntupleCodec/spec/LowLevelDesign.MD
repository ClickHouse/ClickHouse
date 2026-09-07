# Low-Level Design for Per-Subcolumn Codecs

## Scope and assumptions

This document describes the metadata and storage changes needed to implement the selected SQL behavior in `SQLNoModifySubcolumnNoDots.MD`. The parser-facing contract is:

- A column declaration yields its ordinary logical `IDataType` without codec annotations embedded in the type identity.
- It also yields zero or one root codec AST and zero or more element codec operations collected from the local `Tuple` declaration.
- `CREATE TABLE` and `ADD COLUMN` element annotations are concrete codec declarations.
- A typed `ALTER TABLE ... MODIFY COLUMN` targets only the owning top-level column. Its tuple declaration carries a patch in which an element can set `CODEC(...)`, use `REMOVE CODEC`, or omit an operation to preserve the existing declaration.
- A nested tuple element's path is derived by walking the tuple type AST and logical `DataTypeTuple` together. It is never parsed from the `MODIFY COLUMN` target or recovered by splitting text on dots.

The implementation must not recover paths by splitting a formatted identifier on dots. It must not place codec state in `IColumn`, in `IDataType::equals`, or in a pooled `ISerialization` object.

### Parser AST representation

A tuple-element codec operation belongs to the owning `ASTTupleDataType`, not to the element's datatype node. `ASTDataType` retains its ordinary logical-type contract and has no codec fields, flags, formatting, or JSON behavior.

`ASTTupleDataType::children[0]` is the existing element `ASTExpressionList`. For an annotated Tuple, `children[1]` is a sparse `ASTExpressionList` of `ASTTupleElementCodecOperation` nodes. Each operation stores a zero-based element index and a closed `Set` or `Remove` kind. A `Set` has exactly one `ASTFunction` child named `CODEC`; a `Remove` has no children. Duplicate or out-of-range indices and malformed child layouts are rejected. The sparse list avoids one sidecar entry per unannotated element while keeping codec expressions visible to generic AST traversal, cloning, hashing, parameter substitution, and JSON serialization.

One shared AST/logical-type traversal pairs the owning Tuple slot with the corresponding `DataTypeTuple` element. It handles direct Tuple chains, `Array`, and `SimpleAggregateFunction`. Collection, installation, and ALTER patch extraction all use this traversal. An operation after an `Array(...)` element belongs to the outer Tuple slot; an operation inside `Array(Tuple(...))` belongs to that inner Tuple.

`Array` and `SimpleAggregateFunction` are transparent for codec paths. Entering them does not add a segment. For `SimpleAggregateFunction`, traversal pairs its first data-type argument—the second syntax argument—with the customized logical storage type and never treats the aggregate-function name as a type. An annotation reached through `Map`, `Nullable`, `LowCardinality`, or another unsupported wrapper is rejected. This includes `Nested(...)` child declarations represented by `ASTNameTypePair` and typed `JSON(...)` paths represented by `ASTObjectTypeArgument`; validation must traverse those wrapper AST nodes so an annotation cannot be silently omitted. Physical subcolumns such as array sizes, null maps, dictionary indexes, `Dynamic` paths, and `JSON` paths are not declaration targets. They still inherit the policy selected by their logical Tuple path. Outer Array sizes have no Tuple path and therefore use the column root or part default.

## Metadata representation

Add `src/Storages/ColumnCodecDescription.h` and `src/Storages/ColumnCodecDescription.cpp`. Replace `ColumnDescription::codec` in `src/Storages/ColumnsDescription.h` with a value member named `codec` of the following shape:

```cpp
using CodecPath = std::vector<String>;

class ColumnCodecDescription
{
public:
    ColumnCodecDescription() = default;
    ColumnCodecDescription(const ColumnCodecDescription & other);
    ColumnCodecDescription & operator=(const ColumnCodecDescription & other);
    ColumnCodecDescription(ColumnCodecDescription && other);
    ColumnCodecDescription & operator=(ColumnCodecDescription && other);

    struct Declaration
    {
        ASTPtr codec;
        CodecPath declaration_path;
    };

    using CodecsByPath = std::map<CodecPath, ASTPtr>;

    bool empty() const;
    bool hasRoot() const;
    bool hasSubcolumns() const;
    const ASTPtr & getRoot() const;
    const CodecsByPath & getCodecs() const;

    void setRoot(ASTPtr ast);
    void resetRoot();
    void set(CodecPath path, ASTPtr ast);
    void erase(const CodecPath & path);

    Declaration find(const CodecPath & logical_path) const;
    ColumnCodecDescription clone() const;
    bool operator==(const ColumnCodecDescription & rhs) const;
};
```

All explicit declarations use one map: the empty path is the root declaration and non-empty paths are tuple-element declarations. Every map value is non-null. `std::map` compares the path vectors lexicographically, giving deterministic formatting and equality. Copy operations deep-clone every AST. `hasRoot`, `getRoot`, `setRoot`, and `resetRoot` remain convenience operations over the empty-path entry so call sites which specifically mean the column-level declaration remain readable. Initially preserve the current `ColumnDescription` move-assignment ownership behavior as well: clone codec ASTs into the destination and reset them in the source. A later conversion to ordinary container moves is allowed only after auditing all construction and modification paths for assumptions about AST sharing; it is not required for this feature.

`Declaration::codec` is null when no declaration matches. `find` performs only longest-prefix lookup; it does not know about the part default, stream types, structural filtering, or adaptive selection. Those decisions belong to `ColumnCodecResolver`.

The map is exposed only as a const view. Call sites which mean “top-level declaration” use the root convenience methods, while validation and resolver call sites use `find`.

## Canonical logical paths

A canonical path is a vector of tuple-element names relative to the owning top-level storage column. The notation below is diagnostic notation for the internal vector; it is not SQL target syntax:

```text
payload / metadata / source -> {"metadata", "source"}
pair / first position       -> {"1"}
payload / literal-dot name  -> {"metadata.source"}
```

For named tuples, use the exact names in `DataTypeTuple::getElementNames`. For unnamed tuples, use the one-based names returned by `DataTypeTuple::getNameByPosition`. Canonicalization walks the type tree one segment at a time with `DataTypeTuple::tryGetPositionByName` and replaces the supplied segment with `getNameByPosition`. A segment is invalid if the current node is neither a Tuple nor a supported transparent wrapper leading to a Tuple, the element does not exist, or more segments remain after an unsupported non-Tuple element.

Before consuming a path segment, canonicalization passes through ordinary `Array` layers without consuming the segment. It does not unwrap `Nested`, even though `Nested` uses an Array-based logical representation. A `SimpleAggregateFunction` logical type is already its first argument's storage type with a custom name, so Tuple and Array checks operate on that storage type directly.

Put path helpers in `ColumnCodecAST` and `ColumnCodecValidation`, not in the high-fanout policy header:

```cpp
CodecPath canonicalizeCodecPath(const DataTypePtr & root_type, const CodecPath & input);
CodecPath getCodecPath(const ISerialization::SubstreamPath & path);
```

`getCodecPath` collects only entries whose type is `ISerialization::Substream::TupleElement`, using `name_of_substream`. It ignores `ArraySizes`, `NullMap`, `DictionaryKeys`, `VariantElement`, object streams, `QuantizedCodes`, and all other physical details. `SerializationNamed::addToPath` already records tuple element names, so full-column enumeration needs no serialization change.

`NameAndTypePair::name` is not an ownership key. Writers must use `NameAndTypePair::getNameInStorage` to fetch the owning `ColumnDescription`. For a partial `NameAndTypePair`, the resolver matches `getSubcolumnName` through `IDataType::forEachSubcolumn` and stores the callback path as a canonical Tuple prefix. This lookup compares the complete subcolumn name returned by the type API and never splits it. Each stream callback appends its Tuple suffix after removing an identical leading prefix, if present. Wide, Compact, full-column, and partial-column paths therefore share one implementation. Add focused unit coverage for quoted dots and positional tuple elements because a string-splitting implementation can appear to work for ordinary names.

## Validation and normalization

`ColumnCodecValidation` validates exactly one owning type and returns a new normalized policy plus that column's effective stream records rather than mutating its input:

```cpp
struct EffectiveCodecStream;
using EffectiveCodecStreams = std::vector<EffectiveCodecStream>;

struct ColumnCodecValidationResult
{
    ColumnCodecDescription codec;
    std::vector<EffectiveCodecStream> effective_streams;
};
```

Validation and preprocessing must be driven by the effective policy. In particular, do not pass the whole owning type to the root declaration or a whole selected subtree to a subcolumn declaration: either would validate the declaration against descendant streams shadowed by a more specific declaration.

Use a format-independent stream description shared with runtime resolution:

```cpp
struct ApplicableCodecStream
{
    DataTypePtr leaf_type; // null for a structural stream
    CodecPath logical_path;
    bool structural;
};

using ApplicableCodecStreams = std::vector<ApplicableCodecStream>;
```

`ColumnCodecValidation` groups serialization streams by the declaration selected through longest-prefix lookup. For a value stream it validates and instantiates with the leaf type. For a structural stream it uses a null type and `only_generic = true`, exactly like the resolver. A transformation-only component may consequently be absent from a structural stream without invalidating a declaration which also governs value streams.

Type-dependent implicit parameters are substituted into the normalized AST only when all applicable value streams produce the same normalized codec description. If, for example, an unparameterized `Delta` governs both `UInt32` and `UInt64` leaves, their instantiated descriptions differ, so the returned AST retains the unparameterized form and the writer derives the parameter from each leaf. This is the existing compound-type rule, applied to the declaration's effective stream set rather than its syntactic subtree. Structural streams do not prevent substitution of a non-generic component which is filtered out there. `Default` remains `Default` in stored metadata.

A declaration may be dormant because every stream below it is shadowed. It still receives type-independent validation and codec instantiation with a null type, so malformed arguments and chains are rejected before metadata is installed. The stored AST remains in its conservative form: an implicit parameter such as the width in `Delta` is not filled from a null type. Removing a descendant override validates the complete resulting policy again and then normalizes it against the newly governed value streams.

The validation pipeline proceeds as follows:

1. Canonicalize every non-root path with `canonicalizeCodecPath` and reject duplicates after canonicalization.
2. Enumerate the owning logical serialization once. For every value and structural stream, derive its canonical `CodecPath`, perform longest-prefix lookup on the unnormalized policy, and append an `ApplicableCodecStream` to the winning declaration's group. Streams won by the part default are not assigned to a declaration group.
3. Normalize each declaration against every value type in its group. Store the common normalized form when all value streams agree; otherwise keep the generic declaration so runtime can preprocess it per leaf.
4. Instantiate every governed value or structural stream as the final check and return effective-stream records for storage-specific restrictions.

The final pass is authoritative for effective codec/type compatibility. Use an owning record such as:

```cpp
enum class EffectiveCodecSource : uint8_t
{
    Explicit,
    PartDefault,
};

struct EffectiveCodecStream
{
    CodecPath logical_path;
    DataTypePtr leaf_type;
    CodecPath declaration_path;
    ASTPtr normalized_codec;
    bool codec_is_part_default;
    bool structural;
};

```

`normalized_codec` is owned by value and is null only for the symbolic part default. `classifyCodecStream` is the single value-versus-structural primitive used by validation and runtime resolution.

`MergeTreeData::checkProperties` consumes the same records, restricted to the logical paths used by sorting and partition keys, to reject effective lossy codecs. Other restrictions which currently inspect a root `column.codec` must likewise consume effective-stream records. This keeps storage-specific key knowledge out of `ColumnCodecDescription` while preventing those checks from accidentally validating shadowed descendants.

The actual part default is unavailable at DDL time and may later be changed by `RECOMPRESS` TTL, so a missing declaration or winning `CODEC(Default)` has a symbolic `PartDefault` codec identity during this pass. A symbolic default remains useful when reporting the effective policy of each logical path.

Physical stream sharing is deliberately not a DDL validation rule. It depends on the concrete serialization, part format, filename generation, and, for dynamic types, runtime data. MergeTree writers preserve the established behavior for duplicate physical streams: after deriving the final stream key, they return immediately if that stream was already created. The first physical writer therefore owns the stream and its codec. This keeps pre-existing legal schemas, notably flattened `Nested` columns with different sibling codecs, writable after upgrade.

The MergeTree checks for lossy codecs in sorting and partition keys in `MergeTreeData::checkProperties` must enumerate the effective policy per stream. A key that refers to one tuple element must reject a lossy codec on that element; a lossy codec on an unrelated sibling must not reject it. The same audit applies to every current test or restriction that directly inspects `column.codec`.

## AST-to-metadata transfer

Introduce parser-independent conversion helpers:

```cpp
ColumnCodecDescription codecDescriptionFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

void applyCodecDescriptionToAST(
    ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription & codec);
```

`codecDescriptionFromAST` consumes already segmented paths, rejects duplicate declarations, validates and normalizes them, and returns trusted metadata. `applyCodecDescriptionToAST` reconstructs the normalized root and element annotations without changing the logical type.

In `InterpreterCreateQuery::getColumnsDescription`, populate the type first, then call `codecDescriptionFromAST`, then add the `ColumnDescription` to `ColumnsDescription`. The existing alias prohibition applies when the policy is non-empty, not only when the root is present. `InterpreterCreateQuery::formatColumns` must call `applyCodecDescriptionToAST`; assigning only `ASTColumnDeclaration::codec` would lose subcolumn declarations in `SHOW CREATE TABLE`, metadata normalization, backup definitions, and replicated DDL.

Any intermediate AST cloning must clone the codec ASTs. The conversion returns an owning `ColumnCodecDescription`; it must not retain raw AST references or pointers into a short-lived parser tree.

## Copy, equality, and text persistence

Update `ColumnDescription` copy assignment, move assignment, constructors, and `operator==` in `ColumnsDescription.cpp` to delegate to `ColumnCodecDescription`. Copy and, initially, move assignment preserve the existing deep-clone-and-reset codec AST ownership semantics described above. Equality compares canonical paths and normalized AST strings produced with `formatWithSecretsOneLine`; pointer identity is irrelevant.

`ColumnDescription::writeText` and `ColumnDescription::readText` are the replicated `columns` metadata boundary. Root-only policies must keep the existing text form exactly: the ordinary `type->getName` field followed by the ordinary root `CODEC` modifier. This avoids unnecessary metadata differences for all existing tables.

`ColumnDescription::type`, every `NamesAndTypes` entry, and part `columns.txt` type identity remain an undecorated logical `DataTypePtr`. A codec annotation is never accepted as part of an `IDataType` name and is never passed as a codec-bearing string to `DataTypeFactory`.

When subcolumn declarations exist, `ColumnsDescription::toString` writes columns metadata version 2. The logical type field stays equal to `type->getName()`. A separate `TUPLE_ELEMENT_CODECS` escaped field contains the number of entries and, for each entry in deterministic map order, a path-segment count, each escaped segment, and the normalized codec AST text. The root codec remains in the existing modifier field.

`ColumnDescription::readText` accepts version 1 and version 2. Version 2 parses the type with the ordinary datatype grammar and reads the separate policy field. Version 1 also accepts the earlier experimental form with annotations embedded in the escaped type field, extracts it as trusted metadata, and rewrites it as version 2 on the next serialization.

- Formatting is deterministic in tuple declaration order, not user input order.
- Quoting preserves segment boundaries and literal dots.
- A root-only policy serializes in the legacy form.
- A policy with subcolumn declarations uses format version 2 and is never silently reduced to its root.
- Loading legacy metadata without element entries produces a root-only `ColumnCodecDescription` with unchanged semantics; loading version 2 or the earlier experimental decorated version 1 preserves the complete element policy.
- Part `columns.txt` continues to contain only logical names and undecorated types; codec-driven custom serialization restoration consults table metadata separately as it does today.

Use `CodecValidationSettings::trusted` when reading already persisted column metadata, as `ColumnDescription::readText` does today. Normal user `CREATE TABLE`, `ADD COLUMN`, and explicitly changed codec declarations in `MODIFY` use the session codec settings. A separate `enable_tuple_element_codecs` setting, disabled by default, gates only statements which introduce or change persisted tuple-element declarations. Loading, attaching, or restoring existing metadata, retaining or removing declarations, and root-only codec operations are not gated. `MODIFY COLUMN` validates only explicitly supplied or changed declarations with the current session's codec admission settings, then validates the complete resulting policy as trusted metadata for path and type compatibility. Thus retained root and tuple-element declarations are never re-admitted under a different session policy, including when another tuple-element codec changes or when the command changes only a comment, default, TTL, or column setting.

## `ALTER TABLE` application

Keep sparse syntax operations in `AlterCommand`, separate from the complete `ColumnCodecDescription` stored in metadata:

```cpp
enum class ColumnCodecPatchKind : UInt8 { Set, Remove };
struct ColumnCodecPatchOperation { ColumnCodecPatchKind kind; ASTPtr codec; };
using ColumnCodecPatch = std::map<CodecPath, ColumnCodecPatchOperation>;
```

The empty path represents the column-level operation. Non-empty paths represent tuple-element operations. For `ADD COLUMN`, every entry is `Set` and the map is the complete declared policy. For `MODIFY COLUMN`, it is a sparse patch: an absent path preserves the stored declaration. One map entry cannot represent both set and remove.

`AlterCommand::parse` walks the type AST and resulting logical type together through the shared Tuple/Array/SimpleAggregateFunction traversal. It derives canonical paths from tuple nesting and rejects element operations reached through an unsupported wrapper. In `AlterCommand::apply`:

- Fetch the owning top-level `ColumnDescription`.
- Work on a clone of the complete policy.
- Apply every `Remove` by its canonical path.
- Reject `REMOVE CODEC` if that exact old declaration does not exist; an inherited codec does not count.
- Reject duplicate operations while converting the AST to the patch.
- Apply every `Set`, replacing only the matching paths.
- Preserve every omitted tuple-element declaration whose path is still valid in the resulting logical type.
- Run `validateColumnCodecDescriptionForAlter` on the complete resulting policy.
- Rebuild codec-driven custom serializations before installing the column.

Paths have name identity, not positional identity. A tuple element with a declared codec cannot be renamed while removing that declaration in the same typed `MODIFY COLUMN`, because the new declaration contains only the new name. Remove the codec under the old name first, then rename the element in a subsequent command.

The command target remains the owning column name. Neither `MODIFY SUBCOLUMN` nor `MODIFY COLUMN payload.element CODEC(...)` is introduced. A typed `MODIFY COLUMN` must revalidate every retained path against the new type and reject the command if a retained path disappears or a codec becomes incompatible. The sequential metadata snapshot in `AlterCommands::validate` must apply the same rules so multiple modifications in one statement observe preceding ones.

Codec-only changes remain metadata-only. They do not schedule a mutation. Inserts and later merges or mutations use the new policy; old compressed blocks remain readable from their block headers.

## Codec-driven custom serializations

Codec-driven custom serialization is deliberately outside this implementation. `Quantized` currently installs custom serialization only from the root codec and that installation is not path-aware. `codecDescriptionFromAST` therefore rejects `Quantized` on a Tuple element with `NOT_IMPLEMENTED` before metadata is stored. Adding path-aware custom serialization requires rebuilding the affected type subtree without mutating a type object shared through `DataTypeFactory`; it is a follow-up feature.

## Effective-codec resolver

Construct a `ColumnCodecResolver` per written column invocation. The resolver borrows the validated `ColumnCodecDescription` for the synchronous stream-enumeration lifetime, owns the logical `DataTypePtr` and part-default AST, and precomputes a partial written column's Tuple prefix once.

```cpp
class ColumnCodecResolver
{
public:
    ColumnCodecResolver(
        const ColumnCodecDescription & policy,
        DataTypePtr owning_type,
        const NameAndTypePair & written_column,
        ASTPtr part_default,
        bool apply_adaptive_default = false);

    ResolvedCodecDeclaration resolve(
        const ISerialization::SubstreamPath & stream_path) const;

    CompressionCodecPtr getCodec(
        const ISerialization::SubstreamPath & stream_path,
        const CompressionCodecPtr & part_default_codec) const;

private:
    const ColumnCodecDescription & policy;
    DataTypePtr owning_type;
    CodecPath written_column_prefix;
    ASTPtr part_default;
};
```

`resolve` performs longest-prefix lookup. For `{metadata, source, bytes}`, it checks that path, then `{metadata, source}`, then `{metadata}`, then the root, then the part default. `CODEC(Default)` stops the lookup and returns the part default; it does not continue to an ancestor.

`getCodec` then:

1. Uses the stream leaf type and `CompressionCodecFactory::get` when `ISerialization::isSpecialCompressionAllowed` is true.
2. Uses a null type and `only_generic = true` for structural streams.
3. Applies `maybeAdaptiveDefaultCodec` only when `Resolved::codec_is_part_default` is true, the leaf type is an adaptive candidate, and the resolved codec is not encrypting.

`resolve` returns its AST by value. `getCodec` consumes that owning result synchronously; it does not cache a raw `IAST *`, a reference to `Resolved::codec`, or a reference to an entry in `policy`. The `CompressionCodecPtr` passed to `getCodec` is the already resolved part-default codec used for factory substitution and adaptive wrapping; its full AST must match the resolver's owning `part_default`.

Remove the writer-local adaptive/default resolution helper. `MergeTreeDataPartWriterWide::addStreams` and `MergeTreeDataPartWriterCompact::addStreams` receive the complete policy, construct one resolver, and call its `getCodec` for every new physical stream.

The resolver is a write-time policy object. It must not be cached in `IDataType` or `ISerialization`, because the part default can change for each merge due to `RECOMPRESS` TTL.

## MergeTree writer integration

### Wide parts

In `MergeTreeDataPartWriterWide::addStreams`, derive the final stream name and check `column_streams` before codec resolution. If that physical stream was already created, return immediately so its first owner keeps the codec, matching the legacy writer. Otherwise compute the logical path and effective codec and create the stream normally.

`WrittenOffsetSubstreams` remains the `std::set<String>` shared across vertical column writers. A later partial writer skips an `ArraySizes` stream written by an earlier writer before codec resolution. No parallel codec registry is needed: the vertical merge's deterministic physical-column order determines the owner in the same way as before this feature.

All existing mark-codec, maximum block-size, filename hashing, adaptive write-buffer, checksum, and vector-dimension logic remains after codec resolution. Codecs needing an upfront vector dimension still receive a distinct object per physical stream.

### Compact parts

In `MergeTreeDataPartWriterCompact::addStreams`, check `compressed_streams` by final stream name before codec resolution, consistently with the Wide writer. Keep `streams_by_codec` keyed by the instantiated codec hash as today, including the per-stream hash salt for codecs with `needsVectorDimensionUpfront`. Different tuple elements may therefore share one `CompressedStream` when their instantiated codecs are identical and use different streams when they are not.

`compressed_streams` remains the mapping from logical stream filename to `CompressedStream`. A duplicate final stream name reuses its existing entry without resolving or comparing another codec. Compact parts still write one data file; compressed block headers make codec changes between distinct logical streams readable.

### Partial and vertical writes

Today the vertical merge `gathering_columns` are top-level entries obtained from `ColumnsDescription::getAllPhysical`, so the ordinary vertical gather path normally passes a `NameAndTypePair` whose `name == getNameInStorage` and has an empty codec-path prefix. Do not describe that current path as necessarily writing a tuple subcolumn.

Other writer construction and mutation paths can use a `NameAndTypePair` which represents a subcolumn, and future vertical planning may do so. Resolver construction and metadata lookup are therefore centralized defensively: always use `getNameInStorage`, and when `isSubcolumn` is true calculate the canonical prefix by the procedure above. Never call `metadata_snapshot->columns.tryGet(name_and_type.name)` for codec ownership.

Exercise `MergedColumnOnlyOutputStream`, vertical merge stages in `MergeTask`, and partial mutation writer creation in `MutateTask`. Confirm that current top-level gathering columns reach the same per-stream resolver, and add direct coverage of the subcolumn-capable helper even if no current vertical plan produces such an entry. These paths must share the codec registry; no second root-only lookup may remain. A debug assertion should state that every written `NameAndTypePair` resolves to exactly one owning `ColumnDescription` or virtual-column description.

## Structural and shared streams

A declaration on a tuple element owns all streams below that element, including array offsets and null maps. Ownership does not mean every codec in its expression is applicable: `isSpecialCompressionAllowed` and the generic-only factory call remain authoritative. For example, `CODEC(Delta, ZSTD)` on an array element writes its values with `Delta, ZSTD` and its offsets with the generic `ZSTD` component.

Some serializations intentionally make one physical stream reachable from multiple logical paths. The important compatibility case is flattened `Nested`: declarations such as different root codecs on `n.a` and `n.b` have always been legal even though both Arrays reach `n.size0`. Writers continue to deduplicate by the final physical stream key before instantiating the later codec, both within one writer and, for shared offsets, across vertical writers. Thus the first physical column in deterministic write order owns the stream and its codec.

This is the general duplicate-stream rule, not a special conflict-detection exception introduced by tuple-element codecs. In supported ordinary layouts, independent value streams have distinct physical keys and therefore still receive their independently resolved codecs.

## Default, adaptive, and TTL behavior

`MergeTreeData::getCompressionCodecForPart` remains the single source of the part default. Its result already includes `default_compression_codec`, configured codec selection, and an applicable `RECOMPRESS` TTL.

The precedence for every stream is:

```text
deepest subcolumn declaration
    > nearest ancestor declaration
    > root declaration
    > resolved part default
```

An explicit `CODEC(Default)` at any level selects the resolved part default and blocks inheritance. Consequently a `RECOMPRESS` TTL changes streams with no explicit declaration and streams whose winning declaration is `CODEC(Default)`, but it does not override any other root or subcolumn declaration.

Adaptive selection is decided per value stream. An inherited explicit codec is not adaptive. A missing declaration or winning `CODEC(Default)` is adaptive-eligible. Structural streams retain the current generic-only behavior and do not become adaptive merely because a sibling value stream does.

The part's `default_compression_codec.txt` continues to record only the resolved part default. Per-subcolumn policy is table metadata, while the actual codec used by every block remains self-describing on disk.

## Other writers

`IStorage::supportsPerSubcolumnCodecs()` is false by default. MergeTree returns true after its Wide and Compact writers use `ColumnCodecResolver`. `CREATE` and `ALTER` reject a non-root policy for every other engine before installing metadata. `StorageLog` remains unsupported in this change rather than accepting a table that fails on its first insert.

## Introspection

Update `InterpreterDescribeQuery` as follows:

- Its output column is named `codec_expression`. The top-level row prints only `ColumnCodecDescription::getRoot`, preserving current `DESCRIBE TABLE` meaning.
- With `describe_include_subcolumns`, derive the canonical tuple path from the callback path passed to `IDataType::forEachSubcolumn` and print the winning normalized declaration in that subcolumn row's `codec_expression`. Print `Default` when that is the winner; do not substitute a transient TTL codec into schema introspection.
- For structural subcolumns, apply generic-only filtering before displaying a concrete effective expression. If no codec component applies, leave `codec_expression` empty.

`system.columns.compression_codec` keeps its established root-only contract: it prints the root `CODEC(...)` expression or an empty string. `DESCRIBE`'s `codec_expression` is row-relative: root-only on the top-level row and effective-policy output on each requested subcolumn row. `SHOW CREATE TABLE` is the initial complete-policy representation. Additive structured introspection is a follow-up after its public schema is agreed.

Physical introspection is unchanged. Tests should use `mergeTreeCodecBlockCounts` and, where useful, compressed-file/block inspection to prove which codec was written. `system.parts.default_compression_codec` still describes only the part default and must not be presented as the codec of every subcolumn.

## Compatibility and rollout

No data-part read-format change is required. Readers continue to select decompression from compressed block headers, so old and new parts can coexist after a codec-only metadata change.

The metadata rules are:

- A missing policy and a legacy null `codec` both mean no declaration.
- A legacy root AST becomes a policy containing only the empty-path map entry.
- Root-only formatting stays byte-for-byte compatible with the existing normalized column declaration.
- Metadata containing subcolumn annotations uses columns format version 2 and requires a server version that understands it.
- Replication compares the full deterministic policy through `ColumnsDescription::toString`; mismatched subcolumn entries are metadata mismatches.
- Existing parts, detached parts, and parts received from an older replica need no conversion.

`enable_tuple_element_codecs` is the explicit experimental opt-in for adding or changing such metadata. It is not a cluster-wide capability handshake, so an administrator must upgrade all metadata consumers before enabling it and must remove element policies before downgrading. ATTACH and metadata loading do not consult the setting. Do not add a fallback that drops unknown entries; an older server fails explicitly on version 2.

## Test plan

The detailed functional-test matrix, planned stateless test filenames, table definitions, queries, physical codec assertions, and negative syntax cases are in `SQLTestPlan.MD`.

Use new functional tests under `tests/queries/0_stateless`, created with `add-test`. Keep focused unit coverage for `ColumnCodecDescription`, AST extraction/application, and stream-path resolution. Existing top-level codec, adaptive codec, TTL recompression, vertical merge, compact-part, lossy-codec, and `Quantized` suites must continue to pass unchanged; they are the compatibility criterion for root-only policies.
