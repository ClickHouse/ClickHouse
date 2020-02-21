#include <sys/ioctl.h>
#include <unistd.h>
#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Common/PODArray.h>
#include <Common/UTF8Helpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


PrettyBlockOutputFormat::PrettyBlockOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
     : IOutputFormat(header_, out_), format_settings(format_settings_)
{
    struct winsize w;
    if (0 == ioctl(STDOUT_FILENO, TIOCGWINSZ, &w))
        terminal_width = w.ws_col;
}


/// Evaluate the visible width of the values and column names.
/// Note that number of code points is just a rough approximation of visible string width.
void PrettyBlockOutputFormat::calculateWidths(
    const Block & header, const Chunk & chunk,
    WidthsPerColumn & widths, Widths & max_widths, Widths & name_widths)
{
    size_t num_rows = chunk.getNumRows();
    size_t num_columns = chunk.getNumColumns();
    auto & columns = chunk.getColumns();

    widths.resize(num_columns);
    max_widths.resize_fill(num_columns);
    name_widths.resize(num_columns);

    /// Calculate widths of all values.
    String serialized_value;
    size_t prefix = 2; // Tab character adjustment
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto & elem = header.getByPosition(i);
        auto & column = columns[i];

        widths[i].resize(num_rows);

        for (size_t j = 0; j < num_rows; ++j)
        {
            {
                WriteBufferFromString out_(serialized_value);
                elem.type->serializeAsText(*column, j, out_, format_settings);
            }

            widths[i][j] = std::min<UInt64>(format_settings.pretty.max_column_pad_width,
                UTF8::computeWidth(reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size(), prefix));
            max_widths[i] = std::max(max_widths[i], widths[i][j]);
        }

        /// And also calculate widths for names of columns.
        {
            // name string doesn't contain Tab, no need to pass `prefix`
            name_widths[i] = std::min<UInt64>(format_settings.pretty.max_column_pad_width,
                UTF8::computeWidth(reinterpret_cast<const UInt8 *>(elem.name.data()), elem.name.size()));
            max_widths[i] = std::max(max_widths[i], name_widths[i]);
        }
        prefix += max_widths[i] + 3;
    }
}


void PrettyBlockOutputFormat::write(const Chunk & chunk, PortKind port_kind)
{
    UInt64 max_rows = format_settings.pretty.max_rows;

    if (total_rows >= max_rows)
    {
        total_rows += chunk.getNumRows();
        return;
    }

    auto num_rows = chunk.getNumRows();
    auto num_columns = chunk.getNumColumns();
    auto & columns = chunk.getColumns();
    auto & header = getPort(port_kind).getHeader();

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    calculateWidths(header, chunk, widths, max_widths, name_widths);

    /// Create separators
    std::stringstream top_separator;
    std::stringstream middle_names_separator;
    std::stringstream middle_values_separator;
    std::stringstream bottom_separator;

    top_separator           << "┏";
    middle_names_separator  << "┡";
    middle_values_separator << "├";
    bottom_separator        << "└";
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
        {
            top_separator           << "┳";
            middle_names_separator  << "╇";
            middle_values_separator << "┼";
            bottom_separator        << "┴";
        }

        for (size_t j = 0; j < max_widths[i] + 2; ++j)
        {
            top_separator           << "━";
            middle_names_separator  << "━";
            middle_values_separator << "─";
            bottom_separator        << "─";
        }
    }
    top_separator           << "┓\n";
    middle_names_separator  << "┩\n";
    middle_values_separator << "┤\n";
    bottom_separator        << "┘\n";

    std::string top_separator_s = top_separator.str();
    std::string middle_names_separator_s = middle_names_separator.str();
    std::string middle_values_separator_s = middle_values_separator.str();
    std::string bottom_separator_s = bottom_separator.str();

    /// Output the block
    writeString(top_separator_s, out);

    /// Names
    writeCString("┃ ", out);
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeCString(" ┃ ", out);

        auto & col = header.getByPosition(i);

        if (format_settings.pretty.color)
            writeCString("\033[1m", out);

        if (col.type->shouldAlignRightInPrettyFormats())
        {
            for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                writeChar(' ', out);

            writeString(col.name, out);
        }
        else
        {
            writeString(col.name, out);

            for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                writeChar(' ', out);
        }

        if (format_settings.pretty.color)
            writeCString("\033[0m", out);
    }
    writeCString(" ┃\n", out);

    writeString(middle_names_separator_s, out);

    for (size_t i = 0; i < num_rows && total_rows + i < max_rows; ++i)
    {
        if (i != 0)
            writeString(middle_values_separator_s, out);

        writeCString("│ ", out);

        for (size_t j = 0; j < num_columns; ++j)
        {
            if (j != 0)
                writeCString(" │ ", out);

            auto & type = *header.getByPosition(j).type;
            writeValueWithPadding(*columns[j], type, i, widths[j].empty() ? max_widths[j] : widths[j][i], max_widths[j]);
        }

        writeCString(" │\n", out);
    }

    writeString(bottom_separator_s, out);

    total_rows += num_rows;
}


void PrettyBlockOutputFormat::writeValueWithPadding(
        const IColumn & column, const IDataType & type, size_t row_num, size_t value_width, size_t pad_to_width)
{
    auto writePadding = [&]()
    {
        for (size_t k = 0; k < pad_to_width - value_width; ++k)
            writeChar(' ', out);
    };

    if (type.shouldAlignRightInPrettyFormats())
    {
        writePadding();
        type.serializeAsText(column, row_num, out, format_settings);
    }
    else
    {
        type.serializeAsText(column, row_num, out, format_settings);
        writePadding();
    }
}


void PrettyBlockOutputFormat::consume(Chunk chunk)
{
    write(chunk, PortKind::Main);
}

void PrettyBlockOutputFormat::consumeTotals(Chunk chunk)
{
    total_rows = 0;
    writeSuffixIfNot();
    writeCString("\nExtremes:\n", out);
    write(chunk, PortKind::Totals);
}

void PrettyBlockOutputFormat::consumeExtremes(Chunk chunk)
{
    total_rows = 0;
    writeSuffixIfNot();
    writeCString("\nTotals:\n", out);
    write(chunk, PortKind::Extremes);
}


void PrettyBlockOutputFormat::writeSuffix()
{
    if (total_rows >= format_settings.pretty.max_rows)
    {
        writeCString("  Showed first ", out);
        writeIntText(format_settings.pretty.max_rows, out);
        writeCString(".\n", out);
    }
}

void PrettyBlockOutputFormat::finalize()
{
    writeSuffixIfNot();
}


void registerOutputFormatProcessorPretty(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("Pretty", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback,
        const FormatSettings & format_settings)
    {
        return std::make_shared<PrettyBlockOutputFormat>(buf, sample, format_settings);
    });

    factory.registerOutputFormatProcessor("PrettyNoEscapes", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback,
        const FormatSettings & format_settings)
    {
        FormatSettings changed_settings = format_settings;
        changed_settings.pretty.color = false;
        return std::make_shared<PrettyBlockOutputFormat>(buf, sample, changed_settings);
    });
}

}
