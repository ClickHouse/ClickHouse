#pragma once

#include "config.h"

#if USE_ARROW

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/MessageWriter.h>
#include <Processors/Formats/Impl/ArrowIPC/RecordBatchEncoder.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

/// Native ClickHouse writer for the `Arrow` (file) and `ArrowStream` (stream) IPC formats.
///
/// Encodes ClickHouse columns directly into Arrow IPC record-batch buffers and builds the FlatBuffers
/// metadata without the Apache Arrow C++ library. Selected via `output_format_arrow_use_native_writer`.
/// `LowCardinality` is written as its full column, or — when `output_format_arrow_low_cardinality_as_dictionary`
/// is on — as an Arrow dictionary-encoded column (a single dictionary per id, extended across batches via deltas).
class ArrowIPCBlockOutputFormat final : public IOutputFormat
{
public:
    ArrowIPCBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_);

    String getName() const override { return "ArrowIPCBlockOutputFormat"; }

private:
    void consume(Chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void writeSchemaIfNeeded();
    /// Writes one encapsulated message for an encoded batch (a record batch, or a dictionary batch
    /// when `dictionary_id` is set), returning its location for recording an Arrow file `Block`.
    ArrowIPC::MessageWriter::WrittenMessage writeBatchMessage(
        const ArrowIPC::RecordBatchEncoder::EncodedBatch & batch,
        std::optional<int64_t> dictionary_id = std::nullopt,
        bool is_delta = false);

    const bool stream;
    const FormatSettings format_settings;

    Names column_names;
    DataTypes column_types;

    std::optional<ArrowIPC::MessageWriter> message_writer;
    std::unique_ptr<ArrowIPC::RecordBatchEncoder> encoder;
    bool schema_written = false;
    std::vector<ArrowIPC::ArrowFileBlock> dictionary_blocks;
    std::vector<ArrowIPC::ArrowFileBlock> record_blocks;

    /// Dictionary-encoded output (`output_format_arrow_low_cardinality_as_dictionary`): which top-level
    /// columns are dictionary-encoded, and the per-id accumulated dictionary (extended across batches
    /// via Arrow dictionary deltas).
    struct DictionaryColumnState
    {
        MutableColumnPtr values;                              /// accumulated dictionary values (full nested type)
        std::unordered_map<std::string, Int64> value_to_index;
        bool emitted = false;
    };
    std::vector<std::optional<ArrowIPC::OutputDictionary>> column_dictionaries;
    std::vector<DictionaryColumnState> dictionary_states;
};

}

#endif
