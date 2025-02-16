#pragma once

#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>


namespace DB
{

/** Prints the result, aligned with spaces.
  */
class PrettySpaceBlockOutputFormat : public PrettyBlockOutputFormat
{
public:
    PrettySpaceBlockOutputFormat(WriteBuffer & out_, const Block & header, const FormatSettings & format_settings_, bool mono_block_, bool color_)
        : PrettyBlockOutputFormat(out_, header, format_settings_, mono_block_, color_) {}

    String getName() const override { return "PrettySpaceBlockOutputFormat"; }

private:
    void writeChunk(const Chunk & chunk, PortKind port_kind) override;
    void writeSuffix() override;
};

}
