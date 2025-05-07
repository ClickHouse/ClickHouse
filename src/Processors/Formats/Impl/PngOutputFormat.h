#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Formats/PngSerializer.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Port.h>

#include "base/types.h"

namespace DB
{

class PngWriter;

/** A stream for outputting data as PNG image.
  */
class PngOutputFormat final : public IOutputFormat
{
public:
    PngOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_);

    String getName() const override { return "PngOutputFormat"; }

private:
    void writePrefix() override;
    void writeSuffix() override;
    void consume(Chunk) override;

    size_t max_width;
    size_t max_height;

    LoggerPtr log = nullptr;

    FormatSettings format_settings;
    Serializations serializations;
    std::unique_ptr<PngWriter> writer;
    std::unique_ptr<PngSerializer> png_serializer;
    PngPixelFormat output_format;
};

}
