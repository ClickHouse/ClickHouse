#pragma once

#include <Formats/PrettyBlockOutputStream.h>


namespace DB
{

/** Prints the result in the form of beautiful tables, but with fewer delimiter lines.
  */
class PrettyCompactBlockOutputStream : public PrettyBlockOutputStream
{
public:
    PrettyCompactBlockOutputStream(WriteBuffer & ostr_, const Block & header_, const FormatSettings & format_settings)
        : PrettyBlockOutputStream(ostr_, header_, format_settings) {}

    void write(const Block & block) override;

protected:
    void writeHeader(const Block & block, const Widths & max_widths, const Widths & name_widths);
    void writeBottom(const Widths & max_widths);
    void writeRow(size_t row_num, const Block & block, const WidthsPerColumn & widths, const Widths & max_widths);
};

}
