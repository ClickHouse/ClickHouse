#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class WriteBuffer;
class Context;


/** Prints the result in the form of beautiful tables.
  */
class PrettyBlockOutputFormat : public IOutputFormat
{
public:
    /// no_escapes - do not use ANSI escape sequences - to display in the browser, not in the console.
    PrettyBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "PrettyBlockOutputFormat"; }

    void consume(Chunk) override;
    void consumeTotals(Chunk) override;
    void consumeExtremes(Chunk) override;

    void finalize() override;

protected:
    size_t total_rows = 0;
    size_t terminal_width = 0;
    bool suffix_written = false;

    const FormatSettings format_settings;

    using Widths = PODArray<size_t>;
    using WidthsPerColumn = std::vector<Widths>;

    virtual void write(const Chunk & chunk, PortKind port_kind);
    virtual void writeSuffix();


    void writeSuffixIfNot()
    {
        if (!suffix_written)
            writeSuffix();

        suffix_written = true;
    }

    void calculateWidths(
        const Block & header, const Chunk & chunk,
        WidthsPerColumn & widths, Widths & max_widths, Widths & name_widths);

    void writeValueWithPadding(
        const IColumn & column, const IDataType & type, size_t row_num, size_t value_width, size_t pad_to_width);
};

}
