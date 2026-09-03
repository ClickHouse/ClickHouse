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

/// Decoded dictionary values (from `DictionaryBatch` messages), keyed by Arrow dictionary id. Referenced
/// by `RecordBatchDecoder` when materializing dictionary-encoded (LowCardinality) fields.
class DictionaryRegistry
{
public:
    /// A dictionary's values and the type describing them. The type is not always the value field's
    /// natural type: the values are decoded under the requested type hint of the field(s) encoding them
    /// (a `date32` under a numeric target holds raw day numbers, binary under an IPv6 / big-integer
    /// target is already reinterpreted), so the decoder builds `LowCardinality` columns from this pair
    /// instead of re-deriving the type from the referencing field.
    struct Values
    {
        ColumnPtr column;
        DataTypePtr type;
    };

    /// Replaces (or, for delta batches, appends to) the values for a dictionary id. A delta batch is
    /// decoded under the same hint as its base, so it carries the same type.
    void set(Int64 id, ColumnPtr column, DataTypePtr type, bool is_delta);
    const Values & get(Int64 id) const;
    /// Drops all dictionaries (used when an `IInputFormat` is reset to read another stream).
    void clear() { dictionaries.clear(); }

private:
    UnorderedMapWithMemoryTracking<Int64, Values> dictionaries;
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
    /// value type — under `value_hint`, the requested type hint `collectDictionaryValueHints` resolved for
    /// that dictionary (null when it resolved none). The values get the same hint-driven decoding as
    /// inline values in a record batch: a `date32` under a numeric target is read as the raw day number,
    /// a binary leaf under an IPv6 / big-integer target is reinterpreted.
    DecodedColumn decodeDictionaryValues(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body, const ArrowField & value_field,
        const DataTypePtr & value_hint);

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

    /// The requested type hint that decoding a record batch would resolve at each dictionary-encoded field
    /// of the kept top-level fields (all of them when `keep_top_level_fields` is null), keyed by dictionary
    /// id. A dictionary batch is decoded before any record batch and carries no field position to derive a
    /// hint from, so the caller passes the hint collected here when decoding the dictionary's values; they
    /// then get the same hint-driven decoding as the same values inline in a record batch (a `date32` under
    /// a numeric target read as the raw day number, raw binary under an IPv6 / big-integer target
    /// reinterpreted). A dictionary id referenced by several fields gets a hint only when they all resolve
    /// the same one: fields requesting different types cannot share one decoding of the values, so such a
    /// dictionary is decoded to its natural type and each field casts from that. Ids without a hint are
    /// absent from the result.
    UnorderedMapWithMemoryTracking<Int64, DataTypePtr> collectDictionaryValueHints(
        const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields,
        const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_);

private:
    Slice nextBuffer();
    const flatbuf::FieldNode & nextNode();
    /// Length of the next FieldNode without consuming it (for validating a child before decoding it).
    Int64 peekNextNodeLength() const;

    /// The row count the next FieldNode declares, clamped at zero (a negative length is rejected when the
    /// node is consumed). The child of a List/FixedSizeList/Map/Union field is the next node in the
    /// pre-order layout, and its row count is needed to size the child's invisible-rows mask before
    /// `decodeField` consumes the node.
    size_t peekNodeRows() const;

    /// Whether a declared row count is physically impossible for this message: any row of a field that
    /// undergoes value decoding occupies at least one bit in some buffer, so a count above the body's
    /// total bits is forged and must not drive an allocation (e.g. of an invisible-rows mask).
    bool rowCountExceedsBodyBits(size_t rows) const { return rows > total_buffer_bytes * 8; }

    /// The invisible-rows mask for the child of a List/LargeList/Map field, sized to the child's declared
    /// row count. A child row is invisible when only invisible slots reference it, or when no slot
    /// references it at all.
    std::optional<InvisibleRowsMask> buildOffsetsChildInvisibleMask(
        size_t rows, Int64 base, Int64 prev, const PaddedPODArray<UInt64> & offsets,
        const InvisibleRowsMask * invisible_rows) const;

    /// Bounds the declared FieldNode length of a List/LargeList/Map child before it is decoded, so
    /// forged metadata cannot drive an oversized allocation in a buffer-less child subtree (which is
    /// sized by that length alone; see `isBufferlessSubtree`). `prev` is the last referenced offset —
    /// the child row count the parent's offsets actually reference — and `what` names the parent field
    /// in the errors ("list", "map"). Throws `INCORRECT_DATA` when a buffer-less child declares more
    /// rows than are referenced, or when any child declares more rows than the body physically bounds.
    void checkOffsetsChildDeclaredLength(const ArrowField & child, Int64 prev, const char * what) const;

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
    /// The recursive walk of the public `collectDictionaryValueHints`: resolves this field's hint and derives
    /// each child's hint, path and list depth exactly as `decodeInner` / `decodeUnion` do while decoding,
    /// without consuming nodes or buffers. It stops at a dictionary-encoded field: its value subtree is
    /// decoded from the dictionary batch (`decodeDictionaryValues`), where nested fields derive their hints
    /// from the recorded one and no requested types are looked up, so a dictionary nested in another
    /// dictionary's values keeps its natural type. `conflicting` collects the ids whose referencing fields
    /// resolved different hints.
    void collectDictionaryValueHints(
        const ArrowField & field, const DataTypePtr & target_hint, const String & path, size_t list_depth,
        UnorderedMapWithMemoryTracking<Int64, DataTypePtr> & hints, UnorderedSetWithMemoryTracking<Int64> & conflicting) const;
    ColumnPtr decodeUnion(const ArrowField & field, size_t rows, const InvisibleRowsMask * invisible_rows);
    /// `invisible_rows` carries the field's own nulls too (composed by `decodeField` from the same
    /// validity buffer), so this function needs no separate null map for the indices.
    ColumnPtr decodeDictionary(
        const ArrowField & field, size_t rows, bool allow_low_cardinality, const InvisibleRowsMask * invisible_rows);
    ColumnPtr buildNullMap(const Slice & validity, size_t rows, Int64 null_count) const;
    ColumnPtr readOffsetsAndChild(
        const ArrowField & field, size_t rows, bool large, const DataTypePtr & target_hint, const String & path,
        size_t list_depth, const InvisibleRowsMask * invisible_rows);
    /// The requested ClickHouse type for a field, preferring the hint derived from its parent and otherwise
    /// looking up `path` (the dotted column name) in `target_types`. A dotted name reached through
    /// enclosing lists names the flattened column (`Nested(d Int32)` flattens to `n.d Array(Int32)`),
    /// which wraps the element type in one Array per crossed List/Map level; `list_depth` of them are
    /// peeled off the looked-up type so the hint matches this field's own type, the same way the
    /// parent-derived chain unwraps one Array per list. Returns null when no hint is available or the
    /// looked-up type has fewer Array layers than `list_depth`.
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
    /// Decodes one top-level field of the current batch (its node is the next one) after checking that it
    /// declares the batch's row count, and declares it by its ClickHouse type. `target_hint` and
    /// `normalized_name` are the field's requested type hint and lookup path (see `decodeField`).
    DecodedColumn decodeTopLevelColumn(const ArrowField & field, const DataTypePtr & target_hint, const String & normalized_name);
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
    /// (see `rowCountExceedsBodyBits`).
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
