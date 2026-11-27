#pragma once

#include "config.h"

#if USE_VORTEX

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>

struct FFI_VortexWriter;

namespace DB
{

struct VortexWriteContext;

/// Writes Vortex files (https://docs.vortex.dev/) through the Rust bindings in
/// `contrib/vortex-cmake`. Chunks are passed over as Arrow record batches in the C data interface (see
/// `ArrowIPC::exportRecordBatchToC`); the library chooses the
/// encodings and the layout of the file and streams the bytes back through a callback. Unlike
/// reading, this runs entirely on the calling thread.
class VortexBlockOutputFormat final : public IOutputFormat
{
public:
    VortexBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);
    ~VortexBlockOutputFormat() override;

    String getName() const override { return "VortexBlockOutputFormat"; }

private:
    void consume(Chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void initWriter();

    std::unique_ptr<VortexWriteContext> write_context;
    FFI_VortexWriter * writer = nullptr;

    const FormatSettings format_settings;
    /// The settings the chunks are exported to Arrow with.
    const FormatSettings arrow_format_settings;
};

}

#endif
