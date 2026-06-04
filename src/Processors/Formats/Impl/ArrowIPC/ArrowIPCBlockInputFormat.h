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
#include <unordered_map>
#include <vector>

namespace DB
{

class ReadBuffer;

/// Native ClickHouse reader for the `Arrow` (file) and `ArrowStream` (stream) IPC formats.
///
/// Does not use the Apache Arrow C++ library: it parses the IPC metadata (FlatBuffers) directly and
/// decodes record-batch buffers straight into ClickHouse columns. Selected via
/// `input_format_arrow_use_native_reader`. Phase 1 supports the streaming format and flat,
/// uncompressed types; the file format, nested types, dictionaries and compression follow.
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
    void collectDictionaryFields(const std::vector<ArrowIPC::ArrowField> & fields);
    Chunk buildChunk(std::vector<ArrowIPC::RecordBatchDecoder::DecodedColumn> & decoded, size_t num_rows);

    const bool stream;

    ArrowIPC::MessageReader message_reader;
    std::optional<ArrowIPC::ArrowSchema> arrow_schema;
    ArrowIPC::DictionaryRegistry dictionaries;
    /// For each Arrow dictionary id, the field describing its value type (used to decode dictionary batches).
    std::unordered_map<int64_t, ArrowIPC::ArrowField> dictionary_value_fields;
    std::unique_ptr<ArrowIPC::RecordBatchDecoder> decoder;
    bool prepared = false;
    PODArray<char> body_buffer;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;

    const FormatSettings format_settings;

    std::atomic<int> is_stopped{0};
};

}

#endif
