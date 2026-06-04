#pragma once

#include "config.h"

#if USE_ARROW

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{

/// Native ClickHouse writer for the `Arrow` (file) and `ArrowStream` (stream) IPC formats.
///
/// Encodes ClickHouse columns directly into Arrow IPC record-batch buffers and builds the FlatBuffers
/// metadata without the Apache Arrow C++ library. Selected via `output_format_arrow_use_native_writer`.
class ArrowIPCBlockOutputFormat final : public IOutputFormat
{
public:
    ArrowIPCBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_);

    String getName() const override { return "ArrowIPCBlockOutputFormat"; }

private:
    void consume(Chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    /// Whether the output uses the streaming format (`ArrowStream`) rather than the file format (`Arrow`).
    const bool stream;
    const FormatSettings format_settings;
};

}

#endif
