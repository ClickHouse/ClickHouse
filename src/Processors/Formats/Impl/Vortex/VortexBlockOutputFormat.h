#pragma once

#include "config.h"

#if USE_VORTEX

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>

struct FFI_VortexWriter;

namespace DB
{

class CHColumnToArrowColumn;
struct VortexWriteContext;

/// Writes Vortex files (https://docs.vortex.dev/) through the Rust bindings in
/// `rust/workspace/vortex`. Chunks are passed over as Arrow record batches; the library chooses the
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

    void initWriter(const Chunk * chunk);

    std::unique_ptr<VortexWriteContext> write_context;
    std::unique_ptr<CHColumnToArrowColumn> ch_column_to_arrow_column;
    FFI_VortexWriter * writer = nullptr;

    const FormatSettings format_settings;
};

}

#endif
