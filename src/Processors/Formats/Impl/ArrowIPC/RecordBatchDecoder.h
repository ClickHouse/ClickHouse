#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>

namespace DB::ArrowIPC
{

/// Decodes Arrow IPC record batches directly into ClickHouse columns, without the Apache Arrow library.
/// Phase 1 supports flat (non-nested), uncompressed types. The decoder walks the pre-ordered flattened
/// `nodes` (FieldNode) and `buffers` lists exactly as laid out by the Arrow columnar specification and
/// slices the single message body, bounds-checking every access.
class RecordBatchDecoder
{
public:
    RecordBatchDecoder(const ArrowSchema & schema_, const FormatSettings & settings_)
        : schema(schema_), settings(settings_)
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

    /// Decodes one record batch given its metadata and full message body.
    std::vector<DecodedColumn> decodeBatch(const flatbuf::RecordBatch & batch, const PODArray<char> & body);

private:
    Slice nextBuffer();
    const flatbuf::FieldNode & nextNode();

    ColumnPtr decodeField(const ArrowField & field);
    ColumnPtr decodeInner(const ArrowField & field, size_t rows);
    ColumnPtr buildNullMap(const Slice & validity, size_t rows, int64_t null_count) const;

    const ArrowSchema & schema;
    const FormatSettings & settings;

    /// State valid only during a single `decodeBatch` call.
    const flatbuf::RecordBatch * current_batch = nullptr;
    const PODArray<char> * current_body = nullptr;
    size_t node_index = 0;
    size_t buffer_index = 0;
};

}

#endif
