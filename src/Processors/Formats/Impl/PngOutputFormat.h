#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>
#include <Formats/PngWriter.h>
#include <Formats/PngSerializer.h>
#include <Core/Block.h>
#include "base/types.h"

#include <vector>
#include <variant>

namespace DB 
{

/** A stream for outputting data in PNG format.
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

    void validatePixelFormat(const Block &);
            
    size_t width;
    size_t height;
    size_t row_count = 0; 

    FormatSettings format_settings;
    Serializations serializations;
    std::unique_ptr<PngWriter> writer;
    std::unique_ptr<PngSerializer> png_serializer;

    FormatSettings::PixelMode pixel_mode;

};

}
