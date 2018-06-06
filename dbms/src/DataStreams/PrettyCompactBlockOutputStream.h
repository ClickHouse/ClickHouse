#pragma once

#include <DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

/** Prints the result in the form of beautiful tables, but with fewer delimiter lines.
  */
class PrettyCompactBlockOutputStream : public PrettyBlockOutputStream
{
public:
    PrettyCompactBlockOutputStream(WriteBuffer & ostr_, const Block & header_, bool no_escapes_, size_t max_rows_, const Context & context_)
        : PrettyBlockOutputStream(ostr_, header_, no_escapes_, max_rows_, context_) {}

    void write(const Block & block) override;

protected:
    void writeHeader(const Block & block, const Widths & max_widths, const Widths & name_widths);
    void writeBottom(const Widths & max_widths);
    void writeRow(size_t row_num, const Block & block, const WidthsPerColumn & widths, const Widths & max_widths);
};

}
