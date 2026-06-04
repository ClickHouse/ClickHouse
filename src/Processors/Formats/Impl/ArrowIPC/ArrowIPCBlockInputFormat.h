#pragma once

#include "config.h"

#if USE_ARROW

#include <Core/BlockMissingValues.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class ReadBuffer;

/// Native ClickHouse reader for the `Arrow` (file) and `ArrowStream` (stream) IPC formats.
///
/// Unlike `ArrowBlockInputFormat`, this implementation does not use the Apache Arrow C++ library:
/// it parses the IPC metadata (FlatBuffers) directly and decodes the record-batch buffers straight
/// into ClickHouse columns. Selected via `input_format_arrow_use_native_reader`.
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

    /// Whether the input uses the streaming format (`ArrowStream`) rather than the file format (`Arrow`).
    const bool stream;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;

    const FormatSettings format_settings;

    std::atomic<int> is_stopped{0};
};

}

#endif
