#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>

#include <unordered_map>

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

    /// Decodes the schema's fields from one record batch and its full message body.
    std::vector<DecodedColumn> decodeBatch(const flatbuf::RecordBatch & batch, const PODArray<char> & body);

    /// Decodes an explicit list of fields (used for dictionary batches, which carry one value column).
    std::vector<DecodedColumn> decodeColumns(
        const flatbuf::RecordBatch & batch, const PODArray<char> & body, const std::vector<ArrowField> & fields);

private:
    Slice nextBuffer();
    const flatbuf::FieldNode & nextNode();

    ColumnPtr decodeField(const ArrowField & field);
    ColumnPtr decodeInner(const ArrowField & field, size_t rows);
    ColumnPtr decodeDictionary(const ArrowField & field, size_t rows, const Slice & validity, int64_t null_count);
    ColumnPtr buildNullMap(const Slice & validity, size_t rows, int64_t null_count) const;
    ColumnPtr readOffsetsAndChild(const ArrowField & field, size_t rows, bool large);

    const ArrowSchema & schema;
    const FormatSettings & settings;
    const DictionaryRegistry & registry;

    /// State valid only during a single decode call.
    const flatbuf::RecordBatch * current_batch = nullptr;
    const PODArray<char> * current_body = nullptr;
    size_t node_index = 0;
    size_t buffer_index = 0;
};

}

#endif
