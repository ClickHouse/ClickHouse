#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>

#include <unordered_map>
#include <unordered_set>

namespace DB::ArrowIPC
{

/// Decoded dictionary value columns (from `DictionaryBatch` messages), keyed by Arrow dictionary id.
/// Referenced by `RecordBatchDecoder` when materializing dictionary-encoded (LowCardinality) fields.
class DictionaryRegistry
{
public:
    /// Replaces (or, for delta batches, appends to) the values for a dictionary id.
    void set(int64_t id, ColumnPtr values, bool is_delta);
    ColumnPtr get(int64_t id) const;
    /// Drops all dictionaries (used when an `IInputFormat` is reset to read another stream).
    void clear() { dictionaries.clear(); }

private:
    std::unordered_map<int64_t, ColumnPtr> dictionaries;
};

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

    /// A bounds-checked view of one buffer inside the message body.
    struct Slice
    {
        const char * ptr = nullptr;
        int64_t length = 0;
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
    std::vector<DecodedColumn> decodeBatch(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body,
        const std::unordered_set<String> * keep_top_level_fields = nullptr,
        const std::unordered_map<String, DataTypePtr> * target_types = nullptr);

    /// Decodes an explicit list of fields (used for dictionary batches, which carry one value column).
    std::vector<DecodedColumn> decodeColumns(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body, const std::vector<ArrowField> & fields,
        const std::unordered_set<String> * keep_top_level_fields = nullptr,
        const std::unordered_map<String, DataTypePtr> * target_types = nullptr);

private:
    Slice nextBuffer();
    const flatbuf::FieldNode & nextNode();

    /// `allow_low_cardinality` is set only for top-level fields: a dictionary-encoded field decodes into
    /// a LowCardinality column there, but a dictionary nested inside Array/Map/Tuple/Union is materialized
    /// to its plain value column (matching the type `fieldToCHType` declares for the nested field).
    /// `target_hint` is the requested ClickHouse type for this field, derived from the parent's hint as the
    /// decoder recurses (and falling back to a `target_types` lookup by `path`, the dotted column name). It
    /// only affects `date32`: when the hint resolves to a numeric type the raw `Int32` day number is read
    /// without the `Date32` range/overflow check, matching the library reader's numeric type hint.
    ColumnPtr decodeField(
        const ArrowField & field, bool allow_low_cardinality = false,
        const DataTypePtr & target_hint = nullptr, const String & path = {});
    /// Advances the node/buffer/variadic cursors over `field` exactly as `decodeField` would, without
    /// reading or materializing its data. Used to skip an unrequested top-level column while keeping the
    /// flat node/buffer cursors aligned for the columns that follow.
    void skipField(const ArrowField & field);
    ColumnPtr decodeInner(const ArrowField & field, size_t rows, const DataTypePtr & target_hint, const String & path);
    ColumnPtr decodeUnion(const ArrowField & field, size_t rows);
    ColumnPtr decodeDictionary(
        const ArrowField & field, size_t rows, const Slice & validity, int64_t null_count, bool allow_low_cardinality);
    ColumnPtr buildNullMap(const Slice & validity, size_t rows, int64_t null_count) const;
    ColumnPtr readOffsetsAndChild(
        const ArrowField & field, size_t rows, bool large, const DataTypePtr & target_hint, const String & path);
    /// The requested ClickHouse type for a field, preferring the hint derived from its parent and otherwise
    /// looking up `path` (the dotted column name) in `target_types`. Returns null when neither is available.
    DataTypePtr resolveTargetHint(const DataTypePtr & parent_hint, const String & path) const;

    void prepareBuffers(const flatbuf::RecordBatch & batch, const PODArray<char> & body);

    const ArrowSchema & schema;
    const FormatSettings & settings;
    const DictionaryRegistry & registry;

    /// State valid only during a single decode call.
    const flatbuf::RecordBatch * current_batch = nullptr;
    /// Requested column types by normalized (dotted) name, for the recursive `date32` numeric type hint;
    /// null when the caller did not provide them. Points at the caller's map for the call's duration.
    const std::unordered_map<String, DataTypePtr> * target_types = nullptr;
    /// The buffers to decode from: either views into the message body, or into `decompressed_body`.
    std::vector<Slice> buffer_slices;
    PODArray<char> decompressed_body;
    size_t node_index = 0;
    size_t buffer_index = 0;
    /// For BinaryView/Utf8View columns: the per-field count of variadic data buffers.
    std::vector<int64_t> variadic_counts;
    size_t variadic_index = 0;
};

}

#endif
