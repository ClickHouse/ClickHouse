#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace DB::ArrowIPC
{

/// Where a field sits for the requested-type rules of `RecordBatchDecoder::decodeField`: its dotted column
/// name and the number of List/Map levels above it. Together with the requested types of the header, the
/// position determines the type hint a field resolves, and with it how its values decode.
struct FieldPosition
{
    String path;
    size_t list_depth = 0;

    bool operator==(const FieldPosition &) const = default;
};

/// A field encoding a dictionary, as far as decoding the dictionary's values is concerned: the field's
/// position and the requested type hint it resolves there (null when it resolves none).
struct DictionaryUse
{
    FieldPosition position;
    DataTypePtr hint;
};

using DictionaryUses = VectorWithMemoryTracking<DictionaryUse>;

/// Decoded dictionary values (from `DictionaryBatch` messages), keyed by Arrow dictionary id and by the
/// position of the field they were decoded for. Referenced by `RecordBatchDecoder` when materializing
/// dictionary-encoded (LowCardinality) fields.
class DictionaryRegistry
{
public:
    /// A dictionary's values and the type describing them. The type is not always the value field's
    /// natural type: the values are decoded as the field encoding them would decode them inline (a `date32`
    /// under a numeric target holds raw day numbers, binary under an IPv6 / big-integer target is already
    /// reinterpreted), so the decoder builds `LowCardinality` columns from this pair instead of re-deriving
    /// the type from the field. Fields sharing a dictionary id may request different types, so a dictionary
    /// is decoded once per position of a field encoding it and stored per position (see
    /// `RecordBatchDecoder::collectDictionaryUses`).
    struct Values
    {
        ColumnPtr column;
        DataTypePtr type;
    };

    /// Replaces (or, for delta batches, appends to) the values of dictionary `id` decoded for the field at
    /// `position`. A delta batch is decoded for the same positions as its base and must decode to the same
    /// column layout there, so that its values can be appended to the registered column.
    void set(Int64 id, const FieldPosition & position, ColumnPtr column, DataTypePtr type, bool is_delta);
    const Values & get(Int64 id, const FieldPosition & position) const;
    /// Drops all dictionaries (used when an `IInputFormat` is reset to read another stream).
    void clear() { dictionaries.clear(); }

private:
    /// The decodings of one dictionary, keyed by `positionKey`.
    using ValuesByPosition = UnorderedMapWithMemoryTracking<String, Values>;
    static String positionKey(const FieldPosition & position);

    UnorderedMapWithMemoryTracking<Int64, ValuesByPosition> dictionaries;
};

/// Rows whose values are semantically absent: null at this or an ancestor level, in a list range no
/// valid slot references, or in a union child slot its row's type id does not select. The Arrow spec
/// leaves the value bytes of such slots undefined, so value-level validation must not reject them
/// and their values decode as type defaults.
using InvisibleRowsMask = NullMap;

/// The value width of a type whose values are reinterpreted verbatim from raw Arrow binary bytes (IPv6,
/// big integers), or 0 for every other type — including UUID, whose Arrow layout is a byte-swapped
/// fixed_size_binary handled only by the fixed-width converters. The single source of truth for the
/// types the raw-byte converters handle.
size_t rawByteWidth(const WhichDataType & which);

/// Reinterprets the raw bytes of a variable-width binary column (`ColumnString`) as an IPv6 or big
/// integer, matching the Apache Arrow library reader's `readIPv6ColumnFromBinaryData` /
/// `readColumnWithBigNumberFromBinaryData`. `null_map` (may be null) marks rows skipped in the width
/// check and defaulted in the output — the caller passes the composed invisible-rows set, so bytes no
/// one can observe neither fail the check nor force the fallback. Returns nullptr when the target is
/// not one of those types, or when any visible row is not exactly the target width — the column is
/// then left as String for the subsequent cast (matching the library reader's text-parse fallback).
MutableColumnPtr reinterpretStringLeaf(const ColumnString & str, const NullMap * null_map, const DataTypePtr & to_no_null);

/// Navigation helpers for requested-type hints, shared between the decoder's hint recursion and the
/// post-decode raw-byte rewrite in `ArrowIPCBlockInputFormat` — both must resolve the target of a
/// nested field by the same rules, or a leaf the decoder converted comes back to a rewrite that
/// cannot see it.

/// Strips the outer `Nullable`/`LowCardinality` wrappers off a requested-type hint so the underlying
/// type (number, Array, Tuple, Map) can be inspected: `removeLowCardinalityAndNullable` accepting an
/// absent hint.
DataTypePtr stripHint(const DataTypePtr & type);

/// The requested type hint for the element of an Array-like field, or null when the hint is not an Array.
DataTypePtr arrayElementHint(const DataTypePtr & hint);

/// The requested type hint for a struct child. For a named Tuple it is matched by element name — the same
/// way the later named-tuple CAST maps the struct, including case-insensitively when requested — and there
/// is no positional fallback (that could attach the hint to the wrong element). For an unnamed Tuple (the
/// synthetic Map-entries hint) it is matched by position. Null when the hint is not a Tuple or has no match.
DataTypePtr tupleElementHint(const DataTypePtr & hint, const String & child_name, size_t pos, bool case_insensitive);

/// A synthetic Tuple(key, value) hint for a Map's entries struct, or null when the hint is not a Map.
DataTypePtr mapEntriesHint(const DataTypePtr & hint);

/// The union of a row-aligned null map with an optional second one: returns `own` unchanged when
/// `other` is null, otherwise fills `storage` with the element-wise OR and returns it. The inputs are
/// left untouched — `own` typically keeps serving as a column's real null map while the union only
/// drives value decoding.
inline const NullMap * unionNullMaps(const NullMap & own, const NullMap * other, NullMap & storage)
{
    if (!other)
        return &own;
    storage.resize(own.size());
    for (size_t i = 0; i < own.size(); ++i)
        storage[i] = own[i] | (*other)[i];
    return &storage;
}

/// Decodes Arrow IPC record batches directly into ClickHouse columns, without the Apache Arrow library.
/// Supports flat and nested (Array/Tuple/Map) types, LowCardinality (dictionary-encoded) fields, and
/// uncompressed bodies. The decoder walks the pre-ordered flattened `nodes` (FieldNode) and `buffers`
/// lists exactly as laid out by the Arrow columnar specification and slices the single message body,
/// bounds-checking every access.
class RecordBatchDecoder
{
public:
    RecordBatchDecoder(const ArrowSchema & schema_, const FormatSettings & settings_, const DictionaryRegistry & registry_)
        : schema(schema_), settings(settings_), registry(registry_)
    {
    }

    struct DecodedColumn
    {
        String name;
        DataTypePtr type;
        ColumnPtr column;
    };

    using DecodedColumns = VectorWithMemoryTracking<DecodedColumn>;

    /// A bounds-checked view of one buffer inside the message body.
    struct Slice
    {
        const char * ptr = nullptr;
        Int64 length = 0;
    };

    /// Decodes the schema's fields from one record batch and its full message body. When
    /// `keep_top_level_fields` is set, only the named top-level fields are decoded; the others are skipped
    /// (their buffers consumed but not materialized), so a `SELECT` of a subset of columns does not pay
    /// for — or fail on — unrequested columns. The set holds field names normalized the same way the
    /// reader matches them to the header (lower-cased when case-insensitive matching is on).
    /// `target_types` maps each requested column's normalized name (including dotted subcolumn names like
    /// `t.d`) to its requested ClickHouse type. The decoder uses it to read a `date32` mapped to a numeric
    /// target as the raw `Int32` day number without the `Date32` range/overflow check — recursively, so a
    /// `date32` nested in an Array/Tuple/Map or addressed as a subcolumn is handled too — matching the
    /// Apache Arrow library reader's recursive numeric type-hint behavior.
    /// `reachable_buffers`, when set, is a 0/1 mask (see `reachableTopLevelBuffers`) marking the buffers the
    /// requested columns reference; the rest are neither validated nor materialized (they are not in `body`).
    DecodedColumns decodeBatch(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body,
        const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields = nullptr,
        const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types = nullptr,
        const VectorWithMemoryTracking<char> * reachable_buffers = nullptr);

    /// Decodes the single value column of a `DictionaryBatch` — `value_field` describes the dictionary's
    /// value type — as the field encoding the dictionary decodes it inline: at that field's position, under
    /// the requested type hint it resolves there (`use`, see `collectDictionaryUses`), with `target_types_`
    /// to look requested subcolumn types up below it. A `date32` under a numeric target is thus read as the
    /// raw day number, a binary leaf under an IPv6 / big-integer target is reinterpreted, and a dotted
    /// request like `n.d` below the field resolves exactly as it does for inline values.
    DecodedColumn decodeDictionaryValues(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body, const ArrowField & value_field,
        const DictionaryUse & use, const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_);

    /// The buffers (indices into `batch.buffers()`) referenced by the requested top-level fields, as a
    /// 0/1 mask of length `batch.buffers()->size()`. Computed by the same cursor walk decoding uses
    /// (`skipField`), so it stays in lockstep with the decoder's per-field buffer consumption. Used to read,
    /// validate and decompress only the body ranges a subset read actually needs. Returns an all-ones mask
    /// (everything reachable) when `keep_top_level_fields` is null, or if the layout cannot be pre-walked
    /// (the decode path then runs its full validation and reports the precise error).
    VectorWithMemoryTracking<char> reachableTopLevelBuffers(
        const flatbuf::RecordBatch & batch, const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields);

    /// Verifies the batch declares exactly the FieldNodes, buffers and variadic counts that `fields` consume,
    /// using the same cursor walk as decoding (`skipField`). Rejects a malformed batch (surplus or missing)
    /// before its body is materialized, so a dictionary batch carrying buffers beyond its single value field
    /// is not read or decompressed only to be ignored. Throws `INCORRECT_DATA` on a mismatch.
    void validateBatchLayout(const flatbuf::RecordBatch & batch, const ArrowFields & fields);

    /// For every dictionary id the kept top-level fields reference (all fields when `keep_top_level_fields`
    /// is null; at any nesting, including dictionaries nested in another dictionary's values), the distinct
    /// positions of the fields encoding it, each with the requested type hint decoding a record batch would
    /// resolve there. A dictionary batch is decoded before any record batch and has no field position of
    /// its own, so the caller decodes the dictionary's values once per use collected here (see
    /// `decodeDictionaryValues`); each field then finds the values decoded for its own position in the
    /// `DictionaryRegistry`, exactly as if they were inline in a record batch. Ids absent from the result
    /// belong only to unrequested fields.
    UnorderedMapWithMemoryTracking<Int64, DictionaryUses> collectDictionaryUses(
        const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields,
        const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_) const;

private:
    Slice nextBuffer();
    const flatbuf::FieldNode & nextNode();
    /// The FieldNode `offset` nodes past the next one, without consuming anything.
    const flatbuf::FieldNode & peekNode(size_t offset) const;
    /// Length of the next FieldNode without consuming it (for validating a child before decoding it).
    Int64 peekNextNodeLength() const;
    /// Rejects the next FieldNode unless it declares exactly `expected` rows, BEFORE it is decoded: a
    /// buffer-less field is sized by that length alone, so a forged length would otherwise drive an
    /// allocation of that size before any post-decode size check could fire. `what` names the field in the
    /// error, e.g. "struct field 'x'".
    void expectNextNodeLength(size_t expected, const String & what) const;
    /// Rejects the next FieldNode unless it declares at least `minimum` rows: the child of an offsets parent
    /// (List/LargeList/Map) must cover the rows the offsets reference, while a longer one is legal — a
    /// sliced Arrow list keeps the full child. `what` names the field in the error, e.g. "list child".
    void expectNextNodeLengthAtLeast(size_t minimum, const String & what) const;

    /// The row count the next FieldNode declares, clamped at zero (a negative length is rejected when the
    /// node is consumed). The child of a List/FixedSizeList/Map/Union field is the next node in the
    /// pre-order layout, and its row count is needed to size the child's invisible-rows mask before
    /// `decodeField` consumes the node.
    size_t peekNodeRows() const;

    /// Rejects a declared row count the message body cannot physically hold, BEFORE the field is decoded:
    /// any row of a field that undergoes value decoding occupies at least one bit in some buffer, so a
    /// count above the body's total bits is forged, and a buffer-less field declared ahead of its buffered
    /// siblings would otherwise allocate for it before any of their buffer-size checks fires. `what` names
    /// the field in the error, e.g. "list child".
    void checkRowCountWithinBody(size_t rows, const String & what) const;

    /// The invisible-rows mask for the child of a List/LargeList/Map field, sized to the child's declared
    /// row count. A child row is invisible when only invisible slots reference it, or when no slot
    /// references it at all.
    std::optional<InvisibleRowsMask> buildOffsetsChildInvisibleMask(
        size_t rows, Int64 base, Int64 prev, const PaddedPODArray<UInt64> & offsets,
        const InvisibleRowsMask * invisible_rows) const;

    /// Whether the subtree starting at the next node decodes to a column determined by its size alone: it
    /// is buffer-less (see `isBufferlessSubtree`) and none of its struct or fixed-size-list nodes declares
    /// nulls, so every `null` leaf is all NULL and every struct row is valid. A buffer-less subtree that
    /// does declare nulls carries a validity bitmap, which bounds its length physically, and decodes on the
    /// ordinary path.
    bool isSizeDeterminedSubtree(const ArrowField & field) const;
    /// The recursive walk of `isSizeDeterminedSubtree` over the nodes of a buffer-less subtree. `node_offset`
    /// is the subtree's first node relative to the next node and is advanced past the subtree.
    bool bufferlessSubtreeDeclaresNulls(const ArrowField & field, size_t & node_offset) const;
    /// Builds the column a size-determined subtree (see `isSizeDeterminedSubtree`) decodes to, at `rows`
    /// rows instead of the rows its first node declares, consuming its nodes and the validity slots of its
    /// struct and fixed-size-list nodes exactly as `decodeField` would. A parent that keeps only some of the
    /// subtree's rows — an offsets parent dropping the ranges under invisible slots and the unreferenced
    /// head and tail of a sliced list — builds just those, never materializing the declared length, however
    /// large the offsets make it. Nested nodes are still checked against the lengths the decoding path
    /// requires of them, so inconsistent metadata is rejected the same way on both paths; the caller has
    /// validated the first node's length. `target_hint`, `path` and `list_depth` are the subtree's
    /// requested-type position (see `decodeField`) and decide only whether a struct is wrapped in Nullable.
    ColumnPtr buildSizeDeterminedColumn(
        const ArrowField & field, size_t rows, const DataTypePtr & target_hint, const String & path, size_t list_depth);

    /// Consumes and decodes the offsets buffer of a List/LargeList/Map field into ClickHouse array
    /// offsets (per-slot cumulative lengths relative to the first offset), validating that the first
    /// offset is non-negative and that the sequence is monotonic non-decreasing (each offset compared
    /// with its predecessor, not only with the first). `what` names the field in those errors ("list",
    /// "map") and `offsets_what` names its offsets buffer in the buffer-size error.
    ColumnUInt64::MutablePtr decodeListOffsets(
        size_t rows, bool large, const char * what, const char * offsets_what, Int64 & base, Int64 & prev);

    /// `allow_low_cardinality` is set only for top-level fields: a dictionary-encoded field decodes into
    /// a LowCardinality column there, but a dictionary nested inside Array/Map/Tuple/Union is materialized
    /// to its plain value column (matching the type `fieldToCHType` declares for the nested field).
    /// `target_hint` is the requested ClickHouse type for this field, derived from the parent's hint as the
    /// decoder recurses (and falling back to a `target_types` lookup by `path`, the dotted column name). It
    /// only affects `date32`: when the hint resolves to a numeric or Decimal type the raw `Int32` day
    /// number is read without the `Date32` range/overflow check, matching the library reader's numeric
    /// type hint. `list_depth` counts the List/Map levels crossed on the way to this field (see
    /// `resolveTargetHint`). `invisible_rows`, when non-null, is sized to this field's row count (see
    /// `InvisibleRowsMask`); null maps and column types are built from each field's own declared validity
    /// exactly as without a mask.
    ColumnPtr decodeField(
        const ArrowField & field, bool allow_low_cardinality,
        const DataTypePtr & target_hint, const String & path, size_t list_depth,
        const InvisibleRowsMask * invisible_rows);
    /// Advances the node/buffer/variadic cursors over `field` exactly as `decodeField` would, without
    /// reading or materializing its data. Used to skip an unrequested top-level column while keeping the
    /// flat node/buffer cursors aligned for the columns that follow.
    void skipField(const ArrowField & field);
    ColumnPtr decodeInner(
        const ArrowField & field, size_t rows, const DataTypePtr & target_hint, const String & path,
        size_t list_depth, const InvisibleRowsMask * invisible_rows);
    /// The recursive walk of the public `collectDictionaryUses`: resolves this field's hint from
    /// `target_hint`, `path`, `list_depth` and `lookup_types` exactly as `decodeField` does while decoding,
    /// records a use for a dictionary-encoded field, and derives each child's position as `decodeInner` /
    /// `decodeUnion` do, without consuming nodes or buffers. The children of a dictionary-encoded field are
    /// walked like any other's: `decodeDictionaryValues` decodes them at these same positions.
    void collectDictionaryUses(
        const ArrowField & field, const DataTypePtr & target_hint, const String & path, size_t list_depth,
        const UnorderedMapWithMemoryTracking<String, DataTypePtr> * lookup_types,
        UnorderedMapWithMemoryTracking<Int64, DictionaryUses> & uses) const;
    ColumnPtr decodeUnion(const ArrowField & field, size_t rows, const InvisibleRowsMask * invisible_rows);
    /// `invisible_rows` carries the field's own nulls too (composed by `decodeField` from the same
    /// validity buffer), so this function needs no separate null map for the indices. `path` and
    /// `list_depth` are the field's position, which selects the dictionary values decoded for it.
    ColumnPtr decodeDictionary(
        const ArrowField & field, size_t rows, bool allow_low_cardinality, const InvisibleRowsMask * invisible_rows,
        const String & path, size_t list_depth);
    ColumnPtr buildNullMap(const Slice & validity, size_t rows, Int64 null_count) const;
    /// Whether the decoded column of a nullable field gets a Nullable wrapper. Array/Map cannot be inside
    /// Nullable in ClickHouse, so (matching the Apache Arrow library reader) their outer validity is dropped.
    /// A Struct (Tuple) is wrapped only when that is allowed: either `allow_experimental_nullable_tuple_type`
    /// is on, or the requested type at this field (`effective_hint`) is already nullable, e.g. reading into
    /// an existing `Nullable(Tuple)` column — mirroring the library reader's `allow_nullable_struct`.
    /// Otherwise the struct is read as a plain Tuple, dropping the struct-level null map;
    /// `decodeBatchColumn` reconciles the reported type to the column.
    bool wrapsInNullable(const ArrowField & field, const IColumn & inner, const DataTypePtr & effective_hint) const;
    ColumnPtr readOffsetsAndChild(
        const ArrowField & field, size_t rows, bool large, const DataTypePtr & target_hint, const String & path,
        size_t list_depth, const InvisibleRowsMask * invisible_rows);
    /// The requested ClickHouse type for a field: `resolveHint` over the requested types of the batch being
    /// decoded (`target_types`), preferring the hint derived from the parent and otherwise looking up `path`,
    /// the dotted column name, `list_depth` lists below the top level.
    DataTypePtr resolveTargetHint(const DataTypePtr & parent_hint, const String & path, size_t list_depth) const;

    void prepareBuffers(const flatbuf::RecordBatch & batch, const PODArray<char> & body, const VectorWithMemoryTracking<char> * reachable);

    /// Per-batch setup shared by `decodeBatch` and `decodeDictionaryValues`: points the node/buffer cursors
    /// at `batch`, validates its variadic buffer counts and its length, and slices `body` into
    /// `buffer_slices` (see `prepareBuffers`). `target_types_` are the requested types looked up by dotted
    /// column name while this batch decodes (null: no lookups).
    void beginBatch(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body,
        const VectorWithMemoryTracking<char> * reachable_buffers,
        const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_);
    /// Decodes one column of the current batch (its node is the next one) after checking that it declares
    /// the batch's row count, and declares it by its ClickHouse type. `target_hint`, `path` and `list_depth`
    /// are the column's requested-type position (see `decodeField`): a record batch column sits at the top
    /// level, a dictionary's values at the position of the field encoding the dictionary.
    DecodedColumn decodeBatchColumn(
        const ArrowField & field, const DataTypePtr & target_hint, const String & path, size_t list_depth);
    /// Verifies that the batch's nodes, buffers and variadic counts were consumed exactly, then releases the
    /// per-batch state `beginBatch` set up.
    void finishBatch();

    /// A field name as the reader matches it against the header: lower-cased when column matching is
    /// case-insensitive. The keys of `keep_top_level_fields` and `target_types` use the same normalization.
    String normalizedName(const String & name) const;
    /// The dotted name of a struct child for the requested-type lookups (`resolveTargetHint`): the child's
    /// normalized name appended to its parent's path.
    String childPath(const String & path, const String & child_name) const;

    const ArrowSchema & schema;
    const FormatSettings & settings;
    const DictionaryRegistry & registry;

    /// State valid only during a single decode call.
    const flatbuf::RecordBatch * current_batch = nullptr;
    /// Requested column types by normalized (dotted) name, for the recursive `date32` numeric type hint;
    /// null when the caller did not provide them. Points at the caller's map for the call's duration.
    const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types = nullptr;
    /// The buffers to decode from: either views into the message body, or into `decompressed_body`.
    VectorWithMemoryTracking<Slice> buffer_slices;
    /// Total bytes across `buffer_slices`; bounds allocations sized by untrusted FieldNode lengths
    /// (see `checkRowCountWithinBody`).
    size_t total_buffer_bytes = 0;
    PODArray<char> decompressed_body;
    size_t node_index = 0;
    size_t buffer_index = 0;
    /// For BinaryView/Utf8View columns: the per-field count of variadic data buffers.
    VectorWithMemoryTracking<Int64> variadic_counts;
    size_t variadic_index = 0;
};

}

#endif
