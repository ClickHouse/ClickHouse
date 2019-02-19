#pragma once

#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>


namespace DB
{

/** Prints the result in the form of beautiful tables, but with fewer delimiter lines.
  */
class PrettyCompactBlockOutputFormat : public PrettyBlockOutputFormat
{
public:
    PrettyCompactBlockOutputFormat(WriteBuffer & out, Block header, const FormatSettings & format_settings)
        : PrettyBlockOutputFormat(out, std::move(header), format_settings) {}

    String getName() const override { return "PrettyCompactBlockOutputFormat"; }

protected:
    void write(const Chunk & chunk, PortKind port_kind) override;
    void writeHeader(const Block & block, const Widths & max_widths, const Widths & name_widths);
    void writeBottom(const Widths & max_widths);
    void writeRow(
        size_t row_num,
        const Block & header,
        const Columns & columns,
        const WidthsPerColumn & widths,
        const Widths & max_widths);
};

}
