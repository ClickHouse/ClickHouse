#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Formats/Impl/ArrowIPC/BufferCompression.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

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
        VectorWithMemoryTracking<flatbuf::FieldNode> nodes;
        VectorWithMemoryTracking<flatbuf::Buffer> buffers;
        PODArray<char> body;
        Int64 num_rows = 0;
        /// Set when the body buffers were compressed; the writer then adds a BodyCompression to the batch.
        std::optional<CompressionCodec> codec;
    };

    EncodedBatch encode(const Columns & columns, const DataTypes & types, size_t num_rows);

    /// Appends a buffer to the body (8-byte aligned start) and records its {offset, length}.
    void appendBuffer(const void * data, size_t length);

private:
    void encodeField(const IColumn & column, const DataTypePtr & type, size_t num_rows);
    /// `null_map_column` (the outer `ColumnNullable`'s null map, or null for a non-nullable column) lets
    /// leaf encoders skip work for null rows — e.g. `DateTime64` rescaling, which would otherwise overflow
    /// on the arbitrary value a null row may carry.
    void encodeValues(const IColumn & column, const DataTypePtr & type, size_t num_rows, const IColumn * null_map_column = nullptr);
    /// Encodes a Variant column as an Arrow dense union (no validity buffer; a types and an offsets
    /// buffer, the variant children in global order, and a trailing single-element null child).
    void encodeVariant(const IColumn & column, const DataTypePtr & type, size_t num_rows);
    /// Writes a column with no first-class Arrow mapping as an Arrow `Binary` column (an int32 offsets
    /// buffer and the concatenated per-row `getDataAt` bytes), matching the Apache Arrow library writer's
    /// `output_format_arrow_unsupported_types_as_binary` fallback. Read back as `String`. `null_map_column`
    /// (when set) marks rows to emit as zero-length, so a NULL row's nested bytes are not written.
    void encodeAsBinary(const IColumn & column, size_t num_rows, const IColumn * null_map_column = nullptr);

    void appendEmptyBuffer();
    /// Emits the validity buffer: a packed LSB-first bitmap (1 = valid) for nullable columns, or an
    /// empty buffer otherwise. Returns the null count.
    Int64 appendValidity(const IColumn * null_map_column, size_t num_rows);
    void appendOffsets(const IColumn::Offsets & ch_offsets, size_t num_rows);

    const FormatSettings & settings;
    VectorWithMemoryTracking<flatbuf::FieldNode> nodes;
    VectorWithMemoryTracking<flatbuf::Buffer> buffers;
    PODArray<char> body;
};

}

#endif
