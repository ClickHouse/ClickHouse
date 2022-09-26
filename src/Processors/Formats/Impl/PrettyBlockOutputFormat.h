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

protected:
    void consume(Chunk) override;
    void consumeTotals(Chunk) override;
    void consumeExtremes(Chunk) override;

    size_t total_rows = 0;
    size_t terminal_width = 0;

    size_t row_number_width = 7; // "10000. "

    const FormatSettings format_settings;
    Serializations serializations;

    using Widths = PODArray<size_t>;
    using WidthsPerColumn = std::vector<Widths>;

    virtual void write(Chunk chunk, PortKind port_kind);
    void writeSuffix() override;

    void onRowsReadBeforeUpdate() override { total_rows = getRowsReadBefore(); }

    void calculateWidths(
        const Block & header, const Chunk & chunk,
        WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths);

    void writeValueWithPadding(
        const IColumn & column, const ISerialization & serialization, size_t row_num,
        size_t value_width, size_t pad_to_width, bool align_right);
};

}
