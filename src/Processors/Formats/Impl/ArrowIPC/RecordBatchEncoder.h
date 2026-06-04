#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Formats/Impl/ArrowIPC/BufferCompression.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Common/PODArray.h>

#include <optional>

namespace DB::ArrowIPC
{

/// Encodes ClickHouse columns into the buffers of one Arrow IPC record batch, without the Apache
/// Arrow library. The result is a flattened pre-ordered list of FieldNodes and Buffers plus a single
/// 8-byte-aligned body, ready to be written by `MessageWriter`. Supports flat and nested types and
/// uncompressed bodies; `LowCardinality` is materialized to its full column before encoding.
class RecordBatchEncoder
{
public:
    explicit RecordBatchEncoder(const FormatSettings & settings_) : settings(settings_) { }

    struct EncodedBatch
    {
        std::vector<flatbuf::FieldNode> nodes;
        std::vector<flatbuf::Buffer> buffers;
        PODArray<char> body;
        int64_t num_rows = 0;
        /// Set when the body buffers were compressed; the writer then adds a BodyCompression to the batch.
        std::optional<CompressionCodec> codec;
    };

    EncodedBatch encode(const Columns & columns, const DataTypes & types, size_t num_rows);

private:
    void encodeField(const IColumn & column, const DataTypePtr & type, size_t num_rows);
    void encodeValues(const IColumn & column, const DataTypePtr & type, size_t num_rows);

    /// Appends a buffer to the body (8-byte aligned start) and records its {offset, length}.
    void appendBuffer(const void * data, size_t length);
    void appendEmptyBuffer();
    /// Emits the validity buffer: a packed LSB-first bitmap (1 = valid) for nullable columns, or an
    /// empty buffer otherwise. Returns the null count.
    int64_t appendValidity(const IColumn * null_map_column, size_t num_rows);
    void appendOffsets(const IColumn::Offsets & ch_offsets, size_t num_rows);

    const FormatSettings & settings;
    std::vector<flatbuf::FieldNode> nodes;
    std::vector<flatbuf::Buffer> buffers;
    PODArray<char> body;
};

}

#endif
