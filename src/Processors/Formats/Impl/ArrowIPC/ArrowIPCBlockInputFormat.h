#pragma once

#include "config.h"

#if USE_ARROW

#include <Core/BlockMissingValues.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Processors/Formats/Impl/ArrowIPC/RecordBatchDecoder.h>
#include <Formats/FormatSettings.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

class ReadBuffer;
class SeekableReadBuffer;

/// Native ClickHouse reader for the `Arrow` (file) and `ArrowStream` (stream) IPC formats.
///
/// Does not use the Apache Arrow C++ library: it parses the IPC metadata (FlatBuffers) directly and
/// decodes record-batch buffers straight into ClickHouse columns. Selected via
/// `input_format_arrow_use_native_reader`. The streaming format is read sequentially; the file format
/// uses the footer for random access to record batches (seeking the input, or loading it into memory
/// when the input is not seekable).
class ArrowIPCBlockInputFormat final : public IInputFormat
{
public:
    ArrowIPCBlockInputFormat(ReadBuffer & in_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_);

    String getName() const override { return "ArrowIPCBlockInputFormat"; }

    void resetParser() override;

    const BlockMissingValues * getMissingValues() const override { return &block_missing_values; }

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

private:
    Chunk read() override;

    void onCancel() noexcept override { is_stopped = 1; }

    void prepareReader();
    void prepareStreamReader();
    void prepareFileReader();
    void collectDictionaryFields(const std::vector<ArrowIPC::ArrowField> & fields);
    Chunk buildChunk(std::vector<ArrowIPC::RecordBatchDecoder::DecodedColumn> & decoded, size_t num_rows);
    static ColumnPtr convertToHeaderType(
        const ColumnPtr & column, const DataTypePtr & from_type, const DataTypePtr & to_type, const String & name, bool null_as_default);
    Chunk readStream();
    Chunk readFile();

    const bool stream;

    std::optional<ArrowIPC::MessageReader> message_reader;
    std::optional<ArrowIPC::ArrowSchema> arrow_schema;
    ArrowIPC::DictionaryRegistry dictionaries;
    /// For each Arrow dictionary id, the field describing its value type (used to decode dictionary batches).
    std::unordered_map<int64_t, ArrowIPC::ArrowField> dictionary_value_fields;
    std::unique_ptr<ArrowIPC::RecordBatchDecoder> decoder;
    bool prepared = false;
    PODArray<char> body_buffer;

    /// File format: random access to record batches via the footer.
    SeekableReadBuffer * seekable = nullptr;
    String file_data;                       /// Owns the bytes when the input had to be loaded into memory.
    std::unique_ptr<ReadBuffer> memory_buffer;
    struct BlockInfo { Int64 offset = 0; Int64 body_length = 0; };
    std::vector<BlockInfo> record_batch_blocks;
    size_t record_batch_current = 0;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;

    const FormatSettings format_settings;

    std::atomic<int> is_stopped{0};
};

}

#endif
