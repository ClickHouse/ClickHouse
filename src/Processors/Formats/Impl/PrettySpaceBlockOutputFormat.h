#pragma once

#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>


namespace DB
{

/** Prints the result, aligned with spaces.
  */
class PrettySpaceBlockOutputFormat : public PrettyBlockOutputFormat
{
public:
    PrettySpaceBlockOutputFormat(WriteBuffer & out_, const Block & header, const FormatSettings & format_settings_)
        : PrettyBlockOutputFormat(out_, header, format_settings_) {}

    String getName() const override { return "PrettySpaceBlockOutputFormat"; }

private:
    void write(Chunk chunk, PortKind port_kind) override;
    void writeSuffix() override;
};

}
