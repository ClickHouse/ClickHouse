#pragma once

#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>
#include <optional>
#include <unordered_map>


namespace DB
{

/** Prints the result in the form of beautiful tables, but with fewer delimiter lines.
  */
class PrettyCompactBlockOutputFormat : public PrettyBlockOutputFormat
{
public:
    PrettyCompactBlockOutputFormat(WriteBuffer & out_, const Block & header, const FormatSettings & format_settings_, bool mono_block_);
    String getName() const override { return "PrettyCompactBlockOutputFormat"; }

private:
    void writeHeader(const Block & block, const Widths & max_widths, const Widths & name_widths);
    void writeBottom(const Widths & max_widths);
    void writeRow(
        size_t row_num,
        const Block & header,
        const Columns & columns,
        const WidthsPerColumn & widths,
        const Widths & max_widths);

    void writeChunk(const Chunk & chunk, PortKind port_kind) override;
};

}
