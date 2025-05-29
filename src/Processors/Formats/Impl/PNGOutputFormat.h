#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Formats/PNGSerializer.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Port.h>

#include "base/types.h"

namespace DB
{

class PNGWriter;

/** A stream for outputting data as PNG image.
  */
class PNGOutputFormat final : public IOutputFormat
{
public:
    PNGOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_);

    String getName() const override { return "PNGOutputFormat"; }

private:
    void writePrefix() override;
    void writeSuffix() override;
    void consume(Chunk) override;

    LoggerPtr log = nullptr;

    std::unique_ptr<PNGWriter> writer;
    std::unique_ptr<PNGSerializer> serializer;
};

}
