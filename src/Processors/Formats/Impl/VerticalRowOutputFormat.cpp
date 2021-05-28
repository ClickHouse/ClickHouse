#include <cmath>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/VerticalRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Common/UTF8Helpers.h>


namespace DB
{

VerticalRowOutputFormat::VerticalRowOutputFormat(
    WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, params_), format_settings(format_settings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();

    using Widths = std::vector<size_t>;
    Widths name_widths(columns);
    size_t max_name_width = 0;

    String serialized_value;

    for (size_t i = 0; i < columns; ++i)
    {
        /// Note that number of code points is just a rough approximation of visible string width.
        const String & name = sample.getByPosition(i).name;

        name_widths[i] = UTF8::computeWidth(reinterpret_cast<const UInt8 *>(name.data()), name.size());

        if (name_widths[i] > max_name_width)
            max_name_width = name_widths[i];
    }

    names_and_paddings.resize(columns);
    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString buf(names_and_paddings[i]);
        writeString(sample.getByPosition(i).name, buf);
        writeCString(": ", buf);
    }

    for (size_t i = 0; i < columns; ++i)
    {
        size_t new_size = max_name_width - name_widths[i] + names_and_paddings[i].size();
        names_and_paddings[i].resize(new_size, ' ');
    }
}


void VerticalRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    if (row_number > format_settings.pretty.max_rows)
        return;

    writeString(names_and_paddings[field_number], out);
    writeValue(column, serialization, row_num);
    writeChar('\n', out);

    ++field_number;
}


void VerticalRowOutputFormat::writeValue(const IColumn & column, const ISerialization & serialization, size_t row_num) const
{
    serialization.serializeText(column, row_num, out, format_settings);
}


void VerticalRowOutputFormat::writeRowStartDelimiter()
{
    ++row_number;

    if (row_number > format_settings.pretty.max_rows)
        return;

    writeCString("Row ", out);
    writeIntText(row_number, out);
    writeCString(":\n", out);

    size_t width = log10(row_number + 1) + 1 + strlen("Row :");
    for (size_t i = 0; i < width; ++i)
        writeCString("─", out);
    writeChar('\n', out);
}


void VerticalRowOutputFormat::writeRowBetweenDelimiter()
{
    if (row_number > format_settings.pretty.max_rows)
        return;

    writeCString("\n", out);
    field_number = 0;
}


void VerticalRowOutputFormat::writeSuffix()
{
    if (row_number > format_settings.pretty.max_rows)
    {
        writeCString("Showed first ", out);
        writeIntText(format_settings.pretty.max_rows, out);
        writeCString(".\n", out);
    }
}

void VerticalRowOutputFormat::writeBeforeTotals()
{
    writeCString("\n", out);
    writeCString("\n", out);
}

void VerticalRowOutputFormat::writeBeforeExtremes()
{
    if (!was_totals_written)
        writeCString("\n", out);

    writeCString("\n", out);
}

void VerticalRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow(columns, row_num, "Min");
}

void VerticalRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow(columns, row_num, "Max");
}

void VerticalRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeSpecialRow(columns, row_num, "Totals");
    was_totals_written = true;
}

void VerticalRowOutputFormat::writeSpecialRow(const Columns & columns, size_t row_num, const char * title)
{
    row_number = 0;
    field_number = 0;

    size_t num_columns = columns.size();

    writeCString(title, out);
    writeCString(":\n", out);

    size_t width = strlen(title) + 1;
    for (size_t i = 0; i < width; ++i)
        writeCString("─", out);
    writeChar('\n', out);

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }
}

void registerOutputFormatProcessorVertical(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("Vertical", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<VerticalRowOutputFormat>(buf, sample, params, settings);
    });
}

}
