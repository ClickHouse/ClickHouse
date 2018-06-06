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
    PrettyBlockOutputStream(WriteBuffer & ostr_, const Block & header_, bool no_escapes_, size_t max_rows_, const Context & context_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writeSuffix() override;

    void flush() override;

    void setTotals(const Block & totals_) override { totals = totals_; }
    void setExtremes(const Block & extremes_) override { extremes = extremes_; }

protected:
    void writeTotals();
    void writeExtremes();

    WriteBuffer & ostr;
    const Block header;
    size_t max_rows;
    size_t total_rows = 0;
    size_t terminal_width = 0;

    bool no_escapes;

    Block totals;
    Block extremes;

    const Context & context;

    using Widths = PODArray<size_t>;
    using WidthsPerColumn = std::vector<Widths>;

    static void calculateWidths(const Block & block, WidthsPerColumn & widths, Widths & max_widths, Widths & name_widths);
    void writeValueWithPadding(const ColumnWithTypeAndName & elem, size_t row_num, size_t value_width, size_t pad_to_width);
};

}
