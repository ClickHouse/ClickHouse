#pragma once

#include "config.h"

#if USE_LIBPNG

#include <Core/Block_fwd.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{

class PNGSerializer;

/// Output format that renders the result set as a PNG image.
class PNGOutputFormat final : public IOutputFormat
{
public:
    PNGOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_);

    String getName() const override { return "PNG"; }

private:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;

    const FormatSettings format_settings;
    std::unique_ptr<PNGSerializer> serializer;
};

}

#endif
