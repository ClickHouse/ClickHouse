#pragma once

#include "config.h"

#if USE_VORTEX

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>

struct VortexFFIWriter;

namespace DB
{

class CHColumnToArrowColumn;
struct VortexWriteContext;

/// Writes Vortex files (https://github.com/vortex-data/vortex, https://docs.vortex.dev/) through
/// the Rust `vortex` library, see `rust/workspace/vortex`. Chunks are converted to Arrow record
/// batches (the same way as in the Arrow format) and passed to the library over the Arrow C Data
/// Interface. The library compresses the data, chooses the file layout, and streams the bytes of
/// the file back through a callback that writes them to the output buffer.
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
    VortexFFIWriter * writer = nullptr;

    const FormatSettings format_settings;
};

}

#endif
