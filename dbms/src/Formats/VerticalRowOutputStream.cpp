#include <cmath>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/VerticalRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>
#include <Common/UTF8Helpers.h>


namespace DB
{

VerticalRowOutputStream::VerticalRowOutputStream(
    WriteBuffer & ostr_, const Block & sample_, const FormatSettings & format_settings)
    : ostr(ostr_), sample(sample_), format_settings(format_settings)
{
    size_t columns = sample.columns();

    using Widths = std::vector<size_t>;
    Widths name_widths(columns);
    size_t max_name_width = 0;

    String serialized_value;

    for (size_t i = 0; i < columns; ++i)
    {
        /// Note that number of code points is just a rough approximation of visible string width.
        const String & name = sample.getByPosition(i).name;

        name_widths[i] = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(name.data()), name.size());

        if (name_widths[i] > max_name_width)
            max_name_width = name_widths[i];
    }

    names_and_paddings.resize(columns);
    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString out(names_and_paddings[i]);
        writeString(sample.getByPosition(i).name, out);
        writeCString(": ", out);
    }

    for (size_t i = 0; i < columns; ++i)
        names_and_paddings[i].resize(max_name_width + strlen(": "), ' ');
}


void VerticalRowOutputStream::flush()
{
    ostr.next();
}


void VerticalRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    if (row_number > format_settings.pretty.max_rows)
        return;

    writeString(names_and_paddings[field_number], ostr);
    writeValue(column, type, row_num);
    writeChar('\n', ostr);

    ++field_number;
}


void VerticalRowOutputStream::writeValue(const IColumn & column, const IDataType & type, size_t row_num) const
{
    type.serializeText(column, row_num, ostr, format_settings);
}


void VerticalRowOutputStream::writeRowStartDelimiter()
{
    ++row_number;

    if (row_number > format_settings.pretty.max_rows)
        return;

    writeCString("Row ", ostr);
    writeIntText(row_number, ostr);
    writeCString(":\n", ostr);

    size_t width = log10(row_number + 1) + 1 + strlen("Row :");
    for (size_t i = 0; i < width; ++i)
        writeCString("─", ostr);
    writeChar('\n', ostr);
}


void VerticalRowOutputStream::writeRowBetweenDelimiter()
{
    if (row_number > format_settings.pretty.max_rows)
        return;

    writeCString("\n", ostr);
    field_number = 0;
}


void VerticalRowOutputStream::writeSuffix()
{
    if (row_number > format_settings.pretty.max_rows)
    {
        writeCString("Showed first ", ostr);
        writeIntText(format_settings.pretty.max_rows, ostr);
        writeCString(".\n", ostr);
    }

    if (totals || extremes)
    {
        writeCString("\n", ostr);
        writeTotals();
        writeExtremes();
    }
}


void VerticalRowOutputStream::writeSpecialRow(const Block & block, size_t row_num, const char * title)
{
    writeCString("\n", ostr);

    row_number = 0;
    field_number = 0;

    size_t columns = block.columns();

    writeCString(title, ostr);
    writeCString(":\n", ostr);

    size_t width = strlen(title) + 1;
    for (size_t i = 0; i < width; ++i)
        writeCString("─", ostr);
    writeChar('\n', ostr);

    for (size_t i = 0; i < columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        auto & col = block.getByPosition(i);
        writeField(*col.column, *col.type, row_num);
    }
}


void VerticalRowOutputStream::writeTotals()
{
    if (totals)
    {
        writeSpecialRow(totals, 0, "Totals");
    }
}


void VerticalRowOutputStream::writeExtremes()
{
    if (extremes)
    {
        writeSpecialRow(extremes, 0, "Min");
        writeSpecialRow(extremes, 1, "Max");
    }
}


void registerOutputFormatVertical(FormatFactory & factory)
{
    factory.registerOutputFormat("Vertical", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context &,
        const FormatSettings & settings)
    {
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<VerticalRowOutputStream>(buf, sample, settings), sample);
    });
}

}
