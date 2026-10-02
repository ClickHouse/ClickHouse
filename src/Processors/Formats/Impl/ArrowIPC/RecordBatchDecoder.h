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
    /// Each dictionary use stores its decoded values and their resulting ClickHouse type. Requested hints
    /// affect the value representation, so fields sharing a dictionary id are decoded and stored separately
    /// for each requested position. Dictionary entries may be null regardless of the referencing field's
    /// nullability; each field applies its own nullability when materializing these values.
    struct Values
    {
        ColumnPtr column;
        DataTypePtr type;
        /// The value array's null map is preserved when `Array`, `Map`, or plain `Tuple` drops its outer
        /// nullability. Constant maps stay compact; a missing map denotes all-valid entries.
        ColumnPtr null_map;
    };

    /// Adjacent ordinary batches share one segment for direct index lookup. Each constant batch retains
    /// its own segment. `offsets` contains the cumulative logical row count at the end of each segment.
    struct Dictionary
    {
        VectorWithMemoryTracking<Values> segments;
        VectorWithMemoryTracking<size_t> offsets;

        size_t size() const
        {
            return offsets.back();
        }
    };

    /// Replaces (or, for delta batches, appends to) the values of dictionary `id` decoded for the field at
    /// `position`. A delta batch is decoded for the same positions as its base and must decode to the same
    /// column layout there, so referenced values can be gathered into a common output column.
    void set(Int64 id, const FieldPosition & position, Values values, bool is_delta);
    const Dictionary & get(Int64 id, const FieldPosition & position) const;
    /// Drops all dictionaries (used when an `IInputFormat` is reset to read another stream).
    void clear() { dictionaries.clear(); }

private:
    /// The decodings of one dictionary, keyed by `positionKey`.
    using DictionariesByPosition = UnorderedMapWithMemoryTracking<String, Dictionary>;
    static String positionKey(const FieldPosition & position);

    UnorderedMapWithMemoryTracking<Int64, DictionariesByPosition> dictionaries;
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
/// Supports nested types, dictionary encoding, and uncompressed, LZ4, or Zstd bodies. The decoder walks
/// the flattened `FieldNode` and buffer lists in schema order and validates their declared sizes before
/// materializing columns.
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

    /// Decodes requested fields from a record batch, validating every root node's row count. When
    /// `keep_top_level_fields` is set, only the named fields have their values decoded; other fields are
    /// skipped. Names are normalized according to the reader's case-insensitive matching setting.
    /// `target_types` maps normalized column names, including dotted subcolumn names, to requested types.
    /// These hints guide numeric `date32` decoding, raw-byte reinterpretation, and struct nullability at
    /// every nesting level, following the same rules as the Apache Arrow library reader.
    /// `reachable_buffers`, when set, is a 0/1 mask (see `reachableTopLevelBuffers`) marking the buffers the
    /// requested columns reference; the rest are neither validated nor materialized (they are not in `body`).
    DecodedColumns decodeBatch(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body,
        const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields = nullptr,
        const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types = nullptr,
        const VectorWithMemoryTracking<char> * reachable_buffers = nullptr);

    /// Decodes a `DictionaryBatch` value column at the referencing field's position and requested type
    /// hint (`use`), following the same rules as inline values. `target_types_` supplies requested subcolumn
    /// types. The returned null map preserves entry validity independently of the resulting column type.
    DictionaryRegistry::Values decodeDictionaryValues(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body, const ArrowField & value_field,
        const DictionaryUse & use, const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_);

    /// Returns a 0/1 mask of the buffers referenced by the requested top-level fields. The `advanceField`
    /// traversal follows decoding's layout so subset reads can load, validate and decompress only the
    /// needed ranges. Returns an all-ones mask when `keep_top_level_fields` is null or the layout cannot
    /// be traversed; decoding then reports the precise validation error.
    VectorWithMemoryTracking<char> reachableTopLevelBuffers(
        const flatbuf::RecordBatch & batch, const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields);

    /// Verifies that declared node, buffer and variadic counts match the traversal of `fields` by
    /// `advanceField`. Rejects missing or surplus entries before a dictionary batch's body is read or
    /// decompressed. Throws `INCORRECT_DATA` on a mismatch.
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
    /// Returns the `FieldNode` at `offset` from the next node without consuming it.
    const flatbuf::FieldNode & peekNode(size_t offset) const;
    /// Returns the next `FieldNode` length without consuming it.
    Int64 peekNextNodeLength() const;
    /// Validates the next node's row count before decoding or skipping it. `what` identifies the field
    /// in the error message.
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

    /// Checks that a buffered subtree's declared row count fits the body's total bits before decoding.
    /// This also bounds allocations for bufferless fields declared ahead of their buffered siblings.
    /// `what` identifies the field in the error message.
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
    /// Builds a bufferless subtree with `rows` materialized values after the caller validates its root
    /// length. Child lengths and buffer slots are still consumed according to the declared layout.
    /// A root can materialize one value for `ColumnConst`; list and union children materialize only
    /// selected values. Requested tuple conversions retain struct null maps until conversion.
    ColumnPtr buildSizeDeterminedColumn(
        const ArrowField & field, size_t rows, const DataTypePtr & target_hint, const String & path, size_t list_depth);

    /// Consumes and decodes the offsets buffer of a List/LargeList/Map field into ClickHouse array
    /// offsets (per-slot cumulative lengths relative to the first offset), validating that the first
    /// offset is non-negative and that the sequence is monotonic non-decreasing (each offset compared
    /// with its predecessor, not only with the first). `what` names the field in those errors.
    ColumnUInt64::MutablePtr decodeListOffsets(
        size_t rows, bool large, const char * what, Int64 & base, Int64 & prev);

    /// Decodes a field after `decodeBatchColumn` validates its subtree's node lengths and buffer sizes.
    /// `allow_low_cardinality` lets top-level dictionary fields retain `LowCardinality`; nested dictionary
    /// fields are materialized to their plain value column, matching `fieldToCHType`.
    /// `target_hint` supplies the requested type inherited from the parent. `resolveTargetHint` uses it or
    /// looks up the dotted `path` in `target_types`, accounting for the `List`/`Map` levels in `list_depth`.
    /// The resolved hint guides numeric `date32` decoding, raw-byte reinterpretation, and struct nullability.
    /// `invisible_rows`, when present, describes unobservable rows at this field's depth. The field's own
    /// validity determines its null map and column type independently of this inherited mask.
    /// `decoded_null_map`, when provided, receives that null map even when the column drops its `Nullable`
    /// wrapper. A missing map denotes a field with no declared nulls.
    ColumnPtr decodeField(
        const ArrowField & field, bool allow_low_cardinality,
        const DataTypePtr & target_hint, const String & path, size_t list_depth,
        const InvisibleRowsMask * invisible_rows, ColumnUInt8::Ptr * decoded_null_map = nullptr);
    /// Advances the node/buffer/variadic cursors over `field` exactly as `decodeField` would, without
    /// materializing its data. With `validate_lengths`, checks buffer sizes and row-aligned child
    /// lengths before decoding. Otherwise, skips unrequested values without reading their buffers.
    void advanceField(const ArrowField & field, bool validate_lengths = false);
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
    ColumnUInt8::Ptr buildNullMap(const Slice & validity, size_t rows, Int64 null_count) const;
    /// Nullable structs retain their null maps when tuples are nullable or a tuple conversion is
    /// requested. The reader applies the destination nullability after converting visible rows.
    bool wrapsInNullable(const ArrowField & field, const IColumn & inner, const DataTypePtr & effective_hint) const;
    ColumnPtr readOffsetsAndChild(
        const ArrowField & field, size_t rows, bool large, const DataTypePtr & target_hint, const String & path,
        size_t list_depth, const InvisibleRowsMask * invisible_rows);
    /// The requested ClickHouse type for a field: `resolveHint` over the requested types of the batch being
    /// decoded (`target_types`), preferring the hint derived from the parent and otherwise looking up `path`,
    /// the dotted column name, `list_depth` lists below the top level.
    DataTypePtr resolveTargetHint(const DataTypePtr & parent_hint, const String & path, size_t list_depth) const;

    void prepareBuffers(const flatbuf::RecordBatch & batch, const PODArray<char> & body, const VectorWithMemoryTracking<char> * reachable);

    /// Initializes shared state for `decodeBatch` and `decodeDictionaryValues`, validating the batch length
    /// and variadic buffer counts before `prepareBuffers` slices the body. `target_types_` supplies the
    /// requested types looked up by column name during decoding.
    void beginBatch(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body,
        const VectorWithMemoryTracking<char> * reachable_buffers,
        const UnorderedMapWithMemoryTracking<String, DataTypePtr> * target_types_);
    /// Decodes one column of the current batch after the caller has validated its root node's row count,
    /// and reports its ClickHouse type. `target_hint`, `path`, and `list_depth` describe the requested-type
    /// position used by `decodeField`: a record batch column sits at the top level, and dictionary values
    /// sit at the position of the field encoding the dictionary.
    DecodedColumn decodeBatchColumn(
        const ArrowField & field, const DataTypePtr & target_hint, const String & path, size_t list_depth,
        ColumnUInt8::Ptr * decoded_null_map = nullptr);
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
