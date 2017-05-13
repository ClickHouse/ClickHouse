#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class WriteBuffer;
class Context;


/** Prints the result in the form of beautiful tables.
  */
class PrettyBlockOutputStream : public IBlockOutputStream
{
public:
    /// no_escapes - do not use ANSI escape sequences - to display in the browser, not in the console.
    PrettyBlockOutputStream(WriteBuffer & ostr_, bool no_escapes_, size_t max_rows_, const Context & context_);

    void write(const Block & block) override;
    void writeSuffix() override;

    void flush() override;

    void setTotals(const Block & totals_) override { totals = totals_; }
    void setExtremes(const Block & extremes_) override { extremes = extremes_; }

protected:
    void writeTotals();
    void writeExtremes();

    using Widths_t = std::vector<size_t>;

    /// Evaluate the visible width (when outputting to the console with UTF-8 encoding) the width of the values and column names.
    void calculateWidths(Block & block, Widths_t & max_widths, Widths_t & name_widths);

    WriteBuffer & ostr;
    size_t max_rows;
    size_t total_rows = 0;
    size_t terminal_width = 0;

    bool no_escapes;

    Block totals;
    Block extremes;

    const Context & context;
};

}
