#pragma once

#include <DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

/** Prints the result in the form of beautiful tables, but with fewer delimiter lines.
  */
class PrettyCompactBlockOutputStream : public PrettyBlockOutputStream
{
public:
    PrettyCompactBlockOutputStream(WriteBuffer & ostr_, bool no_escapes_, size_t max_rows_, const Context & context_)
        : PrettyBlockOutputStream(ostr_, no_escapes_, max_rows_, context_) {}

    void write(const Block & block) override;

protected:
    void writeHeader(const Block & block, const Widths_t & max_widths, const Widths_t & name_widths);
    void writeBottom(const Widths_t & max_widths);
    void writeRow(size_t row_id, const Block & block, const Widths_t & max_widths, const Widths_t & name_widths);
};

}
