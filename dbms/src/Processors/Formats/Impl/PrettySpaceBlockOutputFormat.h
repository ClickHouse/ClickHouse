#pragma once

#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>


namespace DB
{

/** Prints the result, aligned with spaces.
  */
class PrettySpaceBlockOutputFormat : public PrettyBlockOutputFormat
{
public:
    PrettySpaceBlockOutputFormat(WriteBuffer & out, Block header, const FormatSettings & format_settings)
        : PrettyBlockOutputFormat(out, std::move(header), format_settings) {}

    String getName() const override { return "PrettySpaceBlockOutputFormat"; }

protected:
    void write(const Chunk & chunk, PortKind port_kind) override;
    void writeSuffix() override;
};

}
