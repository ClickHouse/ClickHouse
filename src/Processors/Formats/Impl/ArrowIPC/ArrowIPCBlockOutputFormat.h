#pragma once

#include "config.h"

#if USE_ARROW

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/MessageWriter.h>
#include <Processors/Formats/Impl/ArrowIPC/RecordBatchEncoder.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Core/Names.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

/// Native ClickHouse writer for the `Arrow` (file) and `ArrowStream` (stream) IPC formats.
///
/// Encodes ClickHouse columns directly into Arrow IPC record-batch buffers and builds the FlatBuffers
/// metadata without the Apache Arrow C++ library. Selected via `output_format_arrow_use_native_writer`.
/// `LowCardinality` is written as its full column (matching output_format_arrow_low_cardinality_as_dictionary=false).
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

    const bool stream;
    const FormatSettings format_settings;

    Names column_names;
    DataTypes column_types;

    std::optional<ArrowIPC::MessageWriter> message_writer;
    std::unique_ptr<ArrowIPC::RecordBatchEncoder> encoder;
    bool schema_written = false;
    std::vector<ArrowIPC::ArrowFileBlock> record_blocks;
};

}

#endif
