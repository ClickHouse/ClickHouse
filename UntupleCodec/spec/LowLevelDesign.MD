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

A tuple-element codec operation belongs to the AST node for the element's data type. `ASTDataType` keeps its existing argument representation: when arguments exist, the first child is the `ASTExpressionList`. An optional codec AST is the last child. Codec presence follows from this child layout; one bit in the existing `IAST::flags_storage` records `REMOVE CODEC`. Set and remove are mutually exclusive.

This does not add an arguments vector or a codec pointer to every `ASTDataType`, and it does not duplicate the positional relation with vectors on `ASTTupleDataType`. `getArguments()` continues to return only the expression list, so `DataTypeFactory` never sees the codec as a logical type argument. Each datatype formatter prints its own operation after the type. Clone, tree hash, and AST JSON preserve the operation. Because the codec is an ordinary AST child, generic visitors also reach it, including query-parameter substitution.

This layout also works when support is extended to transparent wrappers such as `Array(Tuple(...))`: the annotation remains on the tuple element type, independent of the number of wrapper nodes above it. That extension must use one shared AST/logical-type traversal for collection, installation, and ALTER patch extraction; the first implementation still rejects annotations below non-Tuple wrappers.

The first implementation restricts declarations to an owning `Tuple` column and elements reached through a direct chain of nested `Tuple` elements. An annotation reached through `Array`, `Map`, `Nullable`, or another non-`Tuple` wrapper is rejected. This includes `Nested(...)` child declarations represented by `ASTNameTypePair` and typed `JSON(...)` paths represented by `ASTObjectTypeArgument`; validation must traverse those wrapper AST nodes so an annotation cannot be silently omitted. Physical subcolumns such as array sizes, null maps, dictionary indexes, `Dynamic` paths, and `JSON` paths are not declaration targets. They still inherit the policy of the nearest containing tuple element.

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

    struct Resolved
    {
        ASTPtr codec;
        CodecPath declaration_path;
        bool codec_is_part_default;
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

    Resolved resolve(const CodecPath & logical_path, ASTPtr part_default) const;
    ColumnCodecDescription clone() const;
    bool operator==(const ColumnCodecDescription & rhs) const;
};
```

All explicit declarations use one map: the empty path is the root declaration and non-empty paths are tuple-element declarations. Every map value is non-null. `std::map` compares the path vectors lexicographically, giving deterministic formatting and equality. Copy operations deep-clone every AST. `hasRoot`, `getRoot`, `setRoot`, and `resetRoot` remain convenience operations over the empty-path entry so call sites which specifically mean the column-level declaration remain readable. Initially preserve the current `ColumnDescription` move-assignment ownership behavior as well: clone codec ASTs into the destination and reset them in the source. A later conversion to ordinary container moves is allowed only after auditing all construction and modification paths for assumptions about AST sharing; it is not required for this feature.

`Resolved::declaration_path` is empty for a root declaration and also for the part default; `codec_is_part_default` disambiguates them. It is true when there is no applicable declaration or when the winning declaration is exactly `CODEC(Default)`. A compound expression such as `CODEC(Delta, Default)` is explicit and therefore has `codec_is_part_default == false`, matching `CompressionCodecFactory::isDefaultCodec`. `Resolved::codec` is an `ASTPtr` returned by value, so it owns an intrusive reference for the entire use of the result; it is never a reference into a map entry.

The map is exposed only as a const view. Call sites which mean “top-level declaration” use the root convenience methods, while writer and effective-policy call sites use `resolve`. This prevents an accidental return to root-only behavior.

## Canonical logical paths

A canonical path is a vector of tuple-element names relative to the owning top-level storage column. The notation below is diagnostic notation for the internal vector; it is not SQL target syntax:

```text
payload / metadata / source -> {"metadata", "source"}
pair / first position       -> {"1"}
payload / literal-dot name  -> {"metadata.source"}
```

For named tuples, use the exact names in `DataTypeTuple::getElementNames`. For unnamed tuples, use the one-based names returned by `DataTypeTuple::getNameByPosition`. Canonicalization walks the type tree one segment at a time with `DataTypeTuple::tryGetPositionByName` and replaces the supplied segment with `getNameByPosition`. It returns both the canonical path and the selected subtree type. A segment is invalid if the current node is not a `DataTypeTuple`, the element does not exist, or more segments remain after a non-tuple element.

Add helpers alongside `ColumnCodecDescription`:

```cpp
struct ResolvedCodecPath
{
    CodecPath path;
    DataTypePtr type;
};

ResolvedCodecPath resolveCodecPath(const DataTypePtr & root_type, const CodecPath & input);
CodecPath getCodecPath(const ISerialization::SubstreamPath & path);
```

`getCodecPath` collects only entries whose type is `ISerialization::Substream::TupleElement`, using `name_of_substream`. It ignores `ArraySizes`, `NullMap`, `DictionaryKeys`, `VariantElement`, object streams, `QuantizedCodes`, and all other physical details. `SerializationNamed::addToPath` already records tuple element names, so full-column enumeration needs no serialization change.

`NameAndTypePair::name` is not an ownership key. Writers must use `NameAndTypePair::getNameInStorage` to fetch the owning `ColumnDescription`. For a partial `NameAndTypePair`, resolve `getSubcolumnName` against the owning type through `IDataType::forEachSubcolumn`; use the matching callback's `SubstreamPath` to obtain a canonical tuple prefix. This lookup compares the complete subcolumn name returned by the type API and never splits it. The callback path emitted by the partial serialization is appended after removing an identical leading prefix, if present. Put this logic in one helper, conceptually:

```cpp
CodecPath getCodecPathForStream(
    const NameAndTypePair & written_column,
    const DataTypePtr & owning_type,
    const ISerialization::SubstreamPath & stream_path);
```

The helper is shared by wide, compact, full-column, and any subcolumn-capable writer path. Add focused unit coverage for quoted dots and positional tuple elements because a string-splitting implementation can appear to work for ordinary names.

## Validation and normalization

Add `ColumnCodecDescription::validateAndNormalize` taking the owning column name, owning type, and `CodecValidationSettings`. It validates exactly one owning column and returns both a new normalized policy and that column's effective logical stream records rather than partially mutating its input:

```cpp
struct EffectiveCodecStream;
using EffectiveCodecStreams = std::vector<EffectiveCodecStream>;

struct ValidatedColumnCodecDescription
{
    ColumnCodecDescription description;
    EffectiveCodecStreams streams;
};
```

Validation and preprocessing must be driven by the effective policy. In particular, do not pass the whole owning type to the root declaration or a whole selected subtree to a subcolumn declaration: either would validate the declaration against descendant streams shadowed by a more specific declaration.

First add a format-independent stream description:

```cpp
struct ApplicableCodecStream
{
    DataTypePtr leaf_type; // null for a structural stream
    bool allow_special;
};

using ApplicableCodecStreams = std::vector<ApplicableCodecStream>;
```

Extend `CompressionCodecFactory` with an overload conceptually named `validateCodecAndGetPreprocessedASTForStreams` that accepts an AST and an explicit collection of applicable streams instead of a compound `DataTypePtr`. Keep the existing overload as a wrapper which enumerates its type and builds this collection, so existing call sites retain their behavior.

The new factory overload performs syntax, codec-order, suspicious-codec, encryption, lossy, and experimental-setting checks once, and instantiates every codec component against only the supplied streams. For a value stream it uses the leaf type. For a structural stream it uses a null type and the same generic-only filtering as `CompressionCodecFactory::get` with `only_generic = true`. A transformation-only component may consequently be absent from a structural stream without invalidating a declaration which also governs value streams.

Type-dependent implicit parameters are substituted into the normalized AST only when all applicable value streams produce the same normalized codec description. If, for example, an unparameterized `Delta` governs both `UInt32` and `UInt64` leaves, their instantiated descriptions differ, so the returned AST retains the unparameterized form and the writer derives the parameter from each leaf. This is the existing compound-type rule, applied to the declaration's effective stream set rather than its syntactic subtree. Structural streams do not prevent substitution of a non-generic component which is filtered out there. `Default` remains `Default` in stored metadata.

A declaration may be temporarily dormant because every stream below it is shadowed. Supporting an empty applicable-stream collection is an intentional new `CompressionCodecFactory` capability, not a call to existing codec creators with a null type. Some current creators require a concrete type and cannot safely be invoked with `nullptr`.

Split the new factory path into two phases. The declaration phase validates AST shape, codec names, type-independent arguments, codec ordering, and feature/settings gates through a registration-level argument validator which does not instantiate a codec. Registrations whose argument parsing currently lives only in a creator must expose that type-independent validation separately. The stream phase invokes creators only for effective value streams with their concrete leaf types; for structural streams it skips non-generic components and invokes only generic creators which support a null type. With no applicable streams, no type-dependent creator is invoked, no type-compatibility check is performed, and no implicit type parameter is invented. The AST remains in a conservative normalized form. Removing a descendant override makes the declaration effective and causes the complete resulting policy to be validated again before metadata is installed.

`validateAndNormalize` then proceeds as follows:

1. Canonicalize every non-root path with `resolveCodecPath` and reject duplicates after canonicalization.
2. Enumerate the owning logical serialization once. For every value and structural stream, derive its canonical `CodecPath`, perform longest-prefix lookup on the unnormalized policy, and append an `ApplicableCodecStream` to the winning declaration's group. Streams won by the part default are not assigned to a declaration group.
3. Call `validateCodecAndGetPreprocessedASTForStreams` once for each declaration with exactly its group, including an empty group for a dormant declaration. Replace that declaration with the returned normalized AST.
4. Enumerate the whole owning serialization again, resolve the normalized effective policy per stream, instantiate every explicit filtered codec, preserve the part default as a symbolic source, and produce the effective-stream records used by storage-specific restriction checks.

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
    ASTPtr codec_ast;
    std::optional<UInt64> explicit_codec_hash;
    EffectiveCodecSource source;
    bool structural;
};

```

`codec_ast` is owned by value and is null only for symbolic `PartDefault`. A common `enumerateEffectiveCodecStreams` helper supplies these records both to `validateAndNormalize` and to storage checks; it must not return references into the candidate policy.

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

When subcolumn declarations exist, `ColumnDescription::writeText` constructs a normalized `ASTColumnDeclaration` from the undecorated type plus the codec-policy metadata using `applyCodecDescriptionToAST`, and serializes that declaration through the column-metadata grammar. The annotation appears syntactically inside the declaration's tuple type, as required by `SQLNoModifySubcolumnNoDots.MD`, but it remains a modifier in the column-declaration AST rather than part of the resulting `IDataType` identity. The existing escaped fields may be refactored to carry this normalized declaration, provided the legacy root-only byte representation remains unchanged.

`ColumnDescription::readText` first recognizes the legacy form or parses the complete normalized column declaration. It then asks the ordinary type-AST conversion to produce an undecorated `DataTypePtr` and separately calls `codecDescriptionFromAST` to extract the policy. It must not call `DataTypeFactory::get` on text containing codec annotations. The precise AST fields and tokens belong to the parser work, but these persistence properties are required:

- Formatting is deterministic in tuple declaration order, not user input order.
- Quoting preserves segment boundaries and literal dots.
- A root-only policy serializes in the legacy form.
- A policy with subcolumn declarations is rejected by an older server through a metadata feature gate rather than being silently reduced to its root.
- Loading existing table `columns` text, ZooKeeper metadata, an `ATTACH` definition, or a backup produces a root-only `ColumnCodecDescription` with unchanged semantics.
- Part `columns.txt` continues to contain only logical names and undecorated types; codec-driven custom serialization restoration consults table metadata separately as it does today.

Use `CodecValidationSettings::trusted` when reading already persisted column metadata, as `ColumnDescription::readText` does today. Normal user `CREATE TABLE`, `ADD COLUMN`, and explicitly changed codec declarations in `MODIFY` use the session codec settings. A separate `enable_tuple_element_codecs` setting, disabled by default, gates only statements which introduce or change persisted tuple-element declarations. Loading and attaching existing metadata, retaining or removing declarations, and root-only codec operations are not gated. `MODIFY COLUMN` validates only explicitly supplied or changed declarations with the current session's codec admission settings, then validates the complete resulting policy as trusted metadata for path and type compatibility. Thus retained root and tuple-element declarations are never re-admitted under a different session policy, including when another tuple-element codec changes or when the command changes only a comment, default, TTL, or column setting.

## `ALTER TABLE` application

Keep sparse syntax operations in `AlterCommand`, separate from the complete `ColumnCodecDescription` stored in metadata:

```cpp
std::map<CodecPath, ASTPtr> codec_sets;
std::vector<CodecPath> codec_removals;
```

The empty path represents the column-level operation in both containers. Non-empty paths represent tuple-element operations. For `ADD COLUMN`, `codec_sets` is the complete declared policy and removals are empty. For `MODIFY COLUMN`, both containers are a sparse patch: a path absent from both preserves the stored declaration.

`AlterCommand::parse` walks the type AST and resulting logical `DataTypeTuple` together. It derives canonical paths from tuple nesting and rejects element operations reached through a non-`Tuple` wrapper. In `AlterCommand::apply`:

- Fetch the owning top-level `ColumnDescription`.
- Work on a clone of the complete policy.
- Apply every explicit removal by its canonical path.
- Reject `REMOVE CODEC` if that exact old declaration does not exist; an inherited codec does not count.
- Reject duplicate removals and a set plus removal of the same path.
- Apply every declaration from `codec_sets`, replacing only the matching paths.
- Preserve every omitted tuple-element declaration whose path is still valid in the resulting logical type.
- Run `validateAndNormalize` on the complete resulting policy.
- Rebuild codec-driven custom serializations before installing the column.

Paths have name identity, not positional identity. A tuple element with a declared codec cannot be renamed while removing that declaration in the same typed `MODIFY COLUMN`, because the new declaration contains only the new name. Remove the codec under the old name first, then rename the element in a subsequent command.

The command target remains the owning column name. Neither `MODIFY SUBCOLUMN` nor `MODIFY COLUMN payload.element CODEC(...)` is introduced. A typed `MODIFY COLUMN` must revalidate every retained path against the new type and reject the command if a retained path disappears or a codec becomes incompatible. The sequential metadata snapshot in `AlterCommands::validate` must apply the same rules so multiple modifications in one statement observe preceding ones.

Codec-only changes remain metadata-only. They do not schedule a mutation. Inserts and later merges or mutations use the new policy; old compressed blocks remain readable from their block headers.

## Codec-driven custom serializations

`attachQuantizeSerializationIfNeeded` in `ColumnsDescription.cpp` currently checks only the root AST and calls `setCustomization` on the top-level type. Replace it with a general, path-aware function:

```cpp
DataTypePtr applyCodecDrivenSerializations(
    const DataTypePtr & logical_type,
    const ColumnCodecDescription & policy,
    const String & column_name);
```

It walks the logical type tree with a `CodecPath`. At each node it resolves the effective codec and invokes codec-specific customization factories. For `Quantized`, `tryExtractQuantizedCodecParams` applies only when the winning declaration at that node contains `Quantized`; the node must be `Array(Float32)`, `Array(Float64)`, or `Array(BFloat16)`. Build a fresh `SerializationQuantizedVector` around that node's default serialization.

Rebuild every enclosing `DataTypeTuple`, `DataTypeArray`, `DataTypeNullable`, and other supported wrapper on the path to the customized node. Preserve tuple names and existing custom names such as `Nested`. Do not mutate a type object obtained from `DataTypeFactory`, because it may be shared by sibling columns. The rebuilding approach in `transformTypesRecursively.cpp` is a useful precedent, but this function must visit complex nodes and must be path-aware.

Call this function whenever a `ColumnDescription` enters or changes in `ColumnsDescription::add` and `ColumnsDescription::modify`, and from `attachQuantizeSerializations` when restoring part `columns.txt`. Make it idempotent by recognizing `ISerialization::getCustomSerializationIdentity`.

The existing MergeTree restriction that `Quantized` is immutable via `ALTER` must become path-aware in `MergeTreeData::checkAlterIsPossible`: compare `(canonical path, Quantized parameters)` signatures across old and new policies. Until an on-disk migration protocol exists, adding, removing, changing, or moving a `Quantized` declaration through `ALTER` remains forbidden, as does changing a type subtree carrying it. Ordinary codecs remain freely alterable. The `Quantized('product', ...)` check in `MergeTreeData::choosePartFormat` must scan all effective tuple paths and force a wide/full part when any materialized subtree has that codec.

## Effective-codec resolver

Construct a `ColumnCodecResolver` per written top-level column. Make it an owning, immutable value for the lifetime of stream initialization: it stores a cloned validated `ColumnCodecDescription`, an owning `DataTypePtr`, and an owning `ASTPtr` obtained from `default_codec->getFullCodecDesc`. This is slightly more copying than borrowing from `StorageMetadataPtr`, but it makes the lifetime contract explicit and prevents a resolver or `Resolved` value from dangling if a metadata handle is replaced.

```cpp
class ColumnCodecResolver
{
public:
    ColumnCodecResolver(
        const ColumnCodecDescription & policy,
        DataTypePtr owning_type,
        ASTPtr part_default);

    ColumnCodecDescription::Resolved resolve(
        const NameAndTypePair & written_column,
        const ISerialization::SubstreamPath & stream_path) const;

    CompressionCodecPtr getCodec(
        const NameAndTypePair & written_column,
        const ISerialization::SubstreamPath & stream_path,
        const CompressionCodecPtr & part_default_codec) const;

private:
    const ColumnCodecDescription policy;
    const DataTypePtr owning_type;
    const ASTPtr part_default;
};
```

`resolve` performs longest-prefix lookup. For `{metadata, source, bytes}`, it checks that path, then `{metadata, source}`, then `{metadata}`, then the root, then the part default. `CODEC(Default)` stops the lookup and returns the part default; it does not continue to an ancestor.

`getCodec` then:

1. Uses the stream leaf type and `CompressionCodecFactory::get` when `ISerialization::isSpecialCompressionAllowed` is true.
2. Uses a null type and `only_generic = true` for structural streams.
3. Applies `maybeAdaptiveDefaultCodec` only when `Resolved::codec_is_part_default` is true, the leaf type is an adaptive candidate, and the resolved codec is not encrypting.

`resolve` returns its AST by value. `getCodec` consumes that owning result synchronously; it does not cache a raw `IAST *`, a reference to `Resolved::codec`, or a reference to an entry in `policy`. The `CompressionCodecPtr` passed to `getCodec` is the already resolved part-default codec used for factory substitution and adaptive wrapping; its full AST must match the resolver's owning `part_default`.

Remove `IMergeTreeDataPartWriter::getCodecDescOrDefault` and `columnUsesDefaultCodec`. Their separate calls cannot express per-stream inheritance reliably. Change `MergeTreeDataPartWriterOnDisk::addStreams`, `MergeTreeDataPartWriterWide::addStreams`, and `MergeTreeDataPartWriterCompact::addStreams` to accept a resolver or fetch it by `getNameInStorage`.

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

Audit all direct reads of `ColumnDescription::codec`. `StorageLog::LogSink::getCodecOrDefault` currently creates one codec for an entire top-level column. Change `LogSink` to enumerate and create streams with `ColumnCodecResolver`, like the MergeTree writers. If a storage engine has one indivisible compressed stream for several tuple elements, reject a policy whose effective codecs differ for that stream during table validation. It must not discard subcolumn entries or apply only the root.

Virtual materialized columns may keep root-only policies initially, but they should be represented by the same type so resolver lookup is uniform. An explicit feature check should reject subcolumn declarations where a virtual-column producer cannot preserve them.

## Introspection

Update `InterpreterDescribeQuery` as follows:

- Its output column is named `codec_expression`. The top-level row prints only `ColumnCodecDescription::getRoot`, preserving current `DESCRIBE TABLE` meaning.
- With `describe_include_subcolumns`, derive the canonical tuple path from the callback path passed to `IDataType::forEachSubcolumn` and print the winning normalized declaration in that subcolumn row's `codec_expression`. Print `Default` when that is the winner; do not substitute a transient TTL codec into schema introspection.
- For structural subcolumns, apply generic-only filtering before displaying a concrete effective expression. If no codec component applies, leave `codec_expression` empty.

`system.columns` uses a different column name and row model: its schema field is `compression_codec`, with one row per top-level table column. For a policy containing subcolumn entries, `system.columns.compression_codec` prints the complete normalized policy in the same canonical syntax as `SHOW CREATE TABLE`; for a root-only policy it preserves the legacy root expression. `DESCRIBE`'s `codec_expression` is instead row-relative: root-only on the top-level row and effective-policy output on each requested subcolumn row. Both paths should share low-level AST argument formatting, but must use distinct policy-versus-row formatting helpers so the two names and semantics are not conflated.

Physical introspection is unchanged. Tests should use `mergeTreeCodecBlockCounts` and, where useful, compressed-file/block inspection to prove which codec was written. `system.parts.default_compression_codec` still describes only the part default and must not be presented as the codec of every subcolumn.

## Compatibility and rollout

No data-part read-format change is required. Readers continue to select decompression from compressed block headers, so old and new parts can coexist after a codec-only metadata change.

The metadata rules are:

- A missing policy and a legacy null `codec` both mean no declaration.
- A legacy root AST becomes a policy containing only the empty-path map entry.
- Root-only formatting stays byte-for-byte compatible with the existing normalized column declaration.
- Metadata containing subcolumn annotations requires a server feature/version gate for replicated databases, backups restored into older versions, and rolling upgrades. Reject creation or alteration until every metadata consumer can parse the representation.
- Replication compares the full deterministic policy through `ColumnsDescription::toString`; mismatched subcolumn entries are metadata mismatches.
- Existing parts, detached parts, and parts received from an older replica need no conversion.

Do not add a fallback that drops unknown entries. Failure to parse or support a non-root policy must be explicit.

## Test plan

The detailed functional-test matrix, planned stateless test filenames, table definitions, queries, physical codec assertions, and negative syntax cases are in `SQLTestPlan.MD`.

Use new functional tests under `tests/queries/0_stateless`, created with `add-test`. Keep focused unit coverage for `ColumnCodecDescription`, AST extraction/application, and stream-path resolution. Existing top-level codec, adaptive codec, TTL recompression, vertical merge, compact-part, lossy-codec, and `Quantized` suites must continue to pass unchanged; they are the compatibility criterion for root-only policies.
