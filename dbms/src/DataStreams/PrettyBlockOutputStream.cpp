#include <sys/ioctl.h>
#include <port/unistd.h>
#include <DataStreams/PrettyBlockOutputStream.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Common/UTF8Helpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


PrettyBlockOutputStream::PrettyBlockOutputStream(
    WriteBuffer & ostr_, const Block & header_, bool no_escapes_, size_t max_rows_, const Context & context_)
     : ostr(ostr_), header(header_), max_rows(max_rows_), no_escapes(no_escapes_), context(context_)
{
    struct winsize w;
    if (0 == ioctl(STDOUT_FILENO, TIOCGWINSZ, &w))
        terminal_width = w.ws_col;
}


void PrettyBlockOutputStream::flush()
{
    ostr.next();
}


/// Evaluate the visible width of the values and column names.
/// Note that number of code points is just a rough approximation of visible string width.
void PrettyBlockOutputStream::calculateWidths(const Block & block, WidthsPerColumn & widths, Widths & max_widths, Widths & name_widths)
{
    size_t rows = block.rows();
    size_t columns = block.columns();

    widths.resize(columns);
    max_widths.resize_fill(columns);
    name_widths.resize(columns);

    /// Calculate widths of all values.
    String serialized_value;
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & elem = block.getByPosition(i);

        widths[i].resize(rows);

        for (size_t j = 0; j < rows; ++j)
        {
            {
                WriteBufferFromString out(serialized_value);
                elem.type->serializeTextEscaped(*elem.column, j, out);
            }

            widths[i][j] = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size());
            max_widths[i] = std::max(max_widths[i], widths[i][j]);
        }

        /// And also calculate widths for names of columns.
        {
            /// We need to obtain length in escaped form.
            {
                WriteBufferFromString out(serialized_value);
                writeEscapedString(elem.name, out);
            }

            name_widths[i] = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size());
            max_widths[i] = std::max(max_widths[i], name_widths[i]);
        }
    }
}


void PrettyBlockOutputStream::write(const Block & block)
{
    if (total_rows >= max_rows)
    {
        total_rows += block.rows();
        return;
    }

    size_t rows = block.rows();
    size_t columns = block.columns();

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    calculateWidths(block, widths, max_widths, name_widths);

    /// Create separators
    std::stringstream top_separator;
    std::stringstream middle_names_separator;
    std::stringstream middle_values_separator;
    std::stringstream bottom_separator;

    top_separator           << "┏";
    middle_names_separator  << "┡";
    middle_values_separator << "├";
    bottom_separator        << "└";
    for (size_t i = 0; i < columns; ++i)
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
    writeString(top_separator_s, ostr);

    /// Names
    writeCString("┃ ", ostr);
    for (size_t i = 0; i < columns; ++i)
    {
        if (i != 0)
            writeCString(" ┃ ", ostr);

        const ColumnWithTypeAndName & col = block.getByPosition(i);

        if (!no_escapes)
            writeCString("\033[1m", ostr);

        if (col.type->shouldAlignRightInPrettyFormats())
        {
            for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                writeChar(' ', ostr);

            writeEscapedString(col.name, ostr);
        }
        else
        {
            writeEscapedString(col.name, ostr);

            for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                writeChar(' ', ostr);
        }

        if (!no_escapes)
            writeCString("\033[0m", ostr);
    }
    writeCString(" ┃\n", ostr);

    writeString(middle_names_separator_s, ostr);

    for (size_t i = 0; i < rows && total_rows + i < max_rows; ++i)
    {
        if (i != 0)
            writeString(middle_values_separator_s, ostr);

        writeCString("│ ", ostr);

        for (size_t j = 0; j < columns; ++j)
        {
            if (j != 0)
                writeCString(" │ ", ostr);

            writeValueWithPadding(block.getByPosition(j), i, widths[j].empty() ? max_widths[j] : widths[j][i], max_widths[j]);
        }

        writeCString(" │\n", ostr);
    }

    writeString(bottom_separator_s, ostr);

    total_rows += rows;
}


void PrettyBlockOutputStream::writeValueWithPadding(const ColumnWithTypeAndName & elem, size_t row_num, size_t value_width, size_t pad_to_width)
{
    auto writePadding = [&]()
    {
        for (size_t k = 0; k < pad_to_width - value_width; ++k)
            writeChar(' ', ostr);
    };

    if (elem.type->shouldAlignRightInPrettyFormats())
    {
        writePadding();
        elem.type->serializeTextEscaped(*elem.column.get(), row_num, ostr);
    }
    else
    {
        elem.type->serializeTextEscaped(*elem.column.get(), row_num, ostr);
        writePadding();
    }
}


void PrettyBlockOutputStream::writeSuffix()
{
    if (total_rows >= max_rows)
    {
        writeCString("  Showed first ", ostr);
        writeIntText(max_rows, ostr);
        writeCString(".\n", ostr);
    }

    total_rows = 0;
    writeTotals();
    writeExtremes();
}


void PrettyBlockOutputStream::writeTotals()
{
    if (totals)
    {
        writeCString("\nTotals:\n", ostr);
        write(totals);
    }
}


void PrettyBlockOutputStream::writeExtremes()
{
    if (extremes)
    {
        writeCString("\nExtremes:\n", ostr);
        write(extremes);
    }
}


}
