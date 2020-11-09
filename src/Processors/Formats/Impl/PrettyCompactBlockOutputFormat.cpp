#include <Common/PODArray.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
///#include <DataStreams/SquashingBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/PrettyCompactBlockOutputFormat.h>


namespace DB
{

namespace ErrorCodes
{


}

void PrettyCompactBlockOutputFormat::writeHeader(
    const Block & block,
    const Widths & max_widths,
    const Widths & name_widths)
{
    /// Names
    writeCString("┌─", out);
    for (size_t i = 0; i < max_widths.size(); ++i)
    {
        if (i != 0)
            writeCString("─┬─", out);

        const ColumnWithTypeAndName & col = block.getByPosition(i);

        if (col.type->shouldAlignRightInPrettyFormats())
        {
            for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                writeCString("─", out);

            if (format_settings.pretty.color)
                writeCString("\033[1m", out);
            writeString(col.name, out);
            if (format_settings.pretty.color)
                writeCString("\033[0m", out);
        }
        else
        {
            if (format_settings.pretty.color)
                writeCString("\033[1m", out);
            writeString(col.name, out);
            if (format_settings.pretty.color)
                writeCString("\033[0m", out);

            for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                writeCString("─", out);
        }
    }
    writeCString("─┐\n", out);
}

void PrettyCompactBlockOutputFormat::writeBottom(const Widths & max_widths)
{
    /// Create delimiters
    std::stringstream bottom_separator;

    bottom_separator << "└";
    for (size_t i = 0; i < max_widths.size(); ++i)
    {
        if (i != 0)
            bottom_separator << "┴";

        for (size_t j = 0; j < max_widths[i] + 2; ++j)
            bottom_separator << "─";
    }
    bottom_separator << "┘\n";

    writeString(bottom_separator.str(), out);
}

void PrettyCompactBlockOutputFormat::writeRow(
    size_t row_num,
    const Block & header,
    const Columns & columns,
    const WidthsPerColumn & widths,
    const Widths & max_widths)
{
    size_t num_columns = max_widths.size();

    writeCString("│ ", out);

    for (size_t j = 0; j < num_columns; ++j)
    {
        if (j != 0)
            writeCString(" │ ", out);

        const auto & type = *header.getByPosition(j).type;
        const auto & cur_widths = widths[j].empty() ? max_widths[j] : widths[j][row_num];
        writeValueWithPadding(*columns[j], type, row_num, cur_widths, max_widths[j]);
    }

    writeCString(" │\n", out);
}

void PrettyCompactBlockOutputFormat::write(const Chunk & chunk, PortKind port_kind)
{
    UInt64 max_rows = format_settings.pretty.max_rows;

    if (total_rows >= max_rows)
    {
        total_rows += chunk.getNumRows();
        return;
    }

    size_t num_rows = chunk.getNumRows();
    const auto & header = getPort(port_kind).getHeader();
    const auto & columns = chunk.getColumns();

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    calculateWidths(header, chunk, widths, max_widths, name_widths);

    writeHeader(header, max_widths, name_widths);

    for (size_t i = 0; i < num_rows && total_rows + i < max_rows; ++i)
        writeRow(i, header, columns, widths, max_widths);

    writeBottom(max_widths);

    total_rows += num_rows;
}


void registerOutputFormatProcessorPrettyCompact(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("PrettyCompact", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback,
        const FormatSettings & format_settings)
    {
        return std::make_shared<PrettyCompactBlockOutputFormat>(buf, sample, format_settings);
    });

    factory.registerOutputFormatProcessor("PrettyCompactNoEscapes", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback,
        const FormatSettings & format_settings)
    {
        FormatSettings changed_settings = format_settings;
        changed_settings.pretty.color = false;
        return std::make_shared<PrettyCompactBlockOutputFormat>(buf, sample, changed_settings);
    });

/// TODO
//    factory.registerOutputFormat("PrettyCompactMonoBlock", [](
//        WriteBuffer & buf,
//        const Block & sample,
//        const FormatSettings & format_settings)
//    {
//        BlockOutputStreamPtr impl = std::make_shared<PrettyCompactBlockOutputFormat>(buf, sample, format_settings);
//        auto res = std::make_shared<SquashingBlockOutputStream>(impl, impl->getHeader(), format_settings.pretty.max_rows, 0);
//        res->disableFlush();
//        return res;
//    });
}

}
