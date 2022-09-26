#include <sys/ioctl.h>
#if defined(OS_SUNOS)
#  include <sys/termios.h>
#endif
#include <unistd.h>
#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/PODArray.h>
#include <Common/UTF8Helpers.h>

namespace DB
{

namespace ErrorCodes
{
}


PrettyBlockOutputFormat::PrettyBlockOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
     : IOutputFormat(header_, out_), format_settings(format_settings_), serializations(header_.getSerializations())
{
    struct winsize w;
    if (0 == ioctl(STDOUT_FILENO, TIOCGWINSZ, &w))
        terminal_width = w.ws_col;
}


/// Evaluate the visible width of the values and column names.
/// Note that number of code points is just a rough approximation of visible string width.
void PrettyBlockOutputFormat::calculateWidths(
    const Block & header, const Chunk & chunk,
    WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths)
{
    size_t num_rows = std::min(chunk.getNumRows(), format_settings.pretty.max_rows);

    /// len(num_rows) + len(". ")
    row_number_width = std::floor(std::log10(num_rows)) + 3;

    size_t num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();

    widths.resize(num_columns);
    max_padded_widths.resize_fill(num_columns);
    name_widths.resize(num_columns);

    /// Calculate widths of all values.
    String serialized_value;
    size_t prefix = 2; // Tab character adjustment
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & elem = header.getByPosition(i);
        const auto & column = columns[i];

        widths[i].resize(num_rows);

        for (size_t j = 0; j < num_rows; ++j)
        {
            {
                WriteBufferFromString out_serialize(serialized_value);
                auto serialization = elem.type->getDefaultSerialization();
                serialization->serializeText(*column, j, out_serialize, format_settings);
            }

            /// Avoid calculating width of too long strings by limiting the size in bytes.
            /// Note that it is just an estimation. 4 is the maximum size of Unicode code point in bytes in UTF-8.
            /// But it's possible that the string is long in bytes but very short in visible size.
            /// (e.g. non-printable characters, diacritics, combining characters)
            if (format_settings.pretty.max_value_width)
            {
                size_t max_byte_size = format_settings.pretty.max_value_width * 4;
                if (serialized_value.size() > max_byte_size)
                    serialized_value.resize(max_byte_size);
            }

            widths[i][j] = UTF8::computeWidth(reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size(), prefix);
            max_padded_widths[i] = std::max<UInt64>(max_padded_widths[i],
                std::min<UInt64>(format_settings.pretty.max_column_pad_width,
                    std::min<UInt64>(format_settings.pretty.max_value_width, widths[i][j])));
        }

        /// And also calculate widths for names of columns.
        {
            // name string doesn't contain Tab, no need to pass `prefix`
            name_widths[i] = std::min<UInt64>(format_settings.pretty.max_column_pad_width,
                UTF8::computeWidth(reinterpret_cast<const UInt8 *>(elem.name.data()), elem.name.size()));
            max_padded_widths[i] = std::max<UInt64>(max_padded_widths[i], name_widths[i]);
        }
        prefix += max_padded_widths[i] + 3;
    }
}

namespace
{

/// Grid symbols are used for printing grid borders in a terminal.
/// Defaults values are UTF-8.
struct GridSymbols
{
    const char * bold_left_top_corner = "┏";
    const char * bold_right_top_corner = "┓";
    const char * left_bottom_corner = "└";
    const char * right_bottom_corner = "┘";
    const char * bold_left_separator = "┡";
    const char * left_separator = "├";
    const char * bold_right_separator = "┩";
    const char * right_separator = "┤";
    const char * bold_top_separator = "┳";
    const char * bold_middle_separator = "╇";
    const char * middle_separator = "┼";
    const char * bottom_separator = "┴";
    const char * bold_dash = "━";
    const char * dash = "─";
    const char * bold_bar = "┃";
    const char * bar = "│";
};

GridSymbols utf8_grid_symbols;

GridSymbols ascii_grid_symbols {
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "-",
    "-",
    "|",
    "|"
};

}


void PrettyBlockOutputFormat::write(Chunk chunk, PortKind port_kind)
{
    UInt64 max_rows = format_settings.pretty.max_rows;

    if (total_rows >= max_rows)
    {
        total_rows += chunk.getNumRows();
        return;
    }

    auto num_rows = chunk.getNumRows();
    auto num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();
    const auto & header = getPort(port_kind).getHeader();

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    calculateWidths(header, chunk, widths, max_widths, name_widths);

    const GridSymbols & grid_symbols = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8 ?
                                       utf8_grid_symbols :
                                       ascii_grid_symbols;

    /// Create separators
    WriteBufferFromOwnString top_separator;
    WriteBufferFromOwnString middle_names_separator;
    WriteBufferFromOwnString middle_values_separator;
    WriteBufferFromOwnString bottom_separator;

    top_separator           << grid_symbols.bold_left_top_corner;
    middle_names_separator  << grid_symbols.bold_left_separator;
    middle_values_separator << grid_symbols.left_separator;
    bottom_separator        << grid_symbols.left_bottom_corner;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
        {
            top_separator           << grid_symbols.bold_top_separator;
            middle_names_separator  << grid_symbols.bold_middle_separator;
            middle_values_separator << grid_symbols.middle_separator;
            bottom_separator        << grid_symbols.bottom_separator;
        }

        for (size_t j = 0; j < max_widths[i] + 2; ++j)
        {
            top_separator           << grid_symbols.bold_dash;
            middle_names_separator  << grid_symbols.bold_dash;
            middle_values_separator << grid_symbols.dash;
            bottom_separator        << grid_symbols.dash;
        }
    }
    top_separator           << grid_symbols.bold_right_top_corner << "\n";
    middle_names_separator  << grid_symbols.bold_right_separator << "\n";
    middle_values_separator << grid_symbols.right_separator << "\n";
    bottom_separator        << grid_symbols.right_bottom_corner << "\n";

    std::string top_separator_s = top_separator.str();
    std::string middle_names_separator_s = middle_names_separator.str();
    std::string middle_values_separator_s = middle_values_separator.str();
    std::string bottom_separator_s = bottom_separator.str();

    if (format_settings.pretty.output_format_pretty_row_numbers)
    {
        /// Write left blank
        writeString(String(row_number_width, ' '), out);
    }
    /// Output the block
    writeString(top_separator_s, out);

    if (format_settings.pretty.output_format_pretty_row_numbers)
    {
        /// Write left blank
        writeString(String(row_number_width, ' '), out);
    }

    /// Names
    writeCString(grid_symbols.bold_bar, out);
    writeCString(" ", out);
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
        {
            writeCString(" ", out);
            writeCString(grid_symbols.bold_bar, out);
            writeCString(" ", out);
        }

        const auto & col = header.getByPosition(i);

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
    writeCString(" ", out);
    writeCString(grid_symbols.bold_bar, out);
    writeCString("\n", out);

    if (format_settings.pretty.output_format_pretty_row_numbers)
    {
        /// Write left blank
        writeString(String(row_number_width, ' '), out);
    }
    writeString(middle_names_separator_s, out);

    for (size_t i = 0; i < num_rows && total_rows + i < max_rows; ++i)
    {
        if (i != 0)
        {
            if (format_settings.pretty.output_format_pretty_row_numbers)
            {
                /// Write left blank
                writeString(String(row_number_width, ' '), out);
            }
            writeString(middle_values_separator_s, out);
        }

        if (format_settings.pretty.output_format_pretty_row_numbers)
        {
            // Write row number;
            auto row_num_string = std::to_string(i + 1) + ". ";
            for (size_t j = 0; j < row_number_width - row_num_string.size(); ++j)
            {
                writeCString(" ", out);
            }
            writeString(row_num_string, out);
        }

        writeCString(grid_symbols.bar, out);

        for (size_t j = 0; j < num_columns; ++j)
        {
            if (j != 0)
                writeCString(grid_symbols.bar, out);
            const auto & type = *header.getByPosition(j).type;
            writeValueWithPadding(*columns[j], *serializations[j], i,
                widths[j].empty() ? max_widths[j] : widths[j][i],
                max_widths[j], type.shouldAlignRightInPrettyFormats());
        }

        writeCString(grid_symbols.bar, out);
        writeCString("\n", out);
    }

    if (format_settings.pretty.output_format_pretty_row_numbers)
    {
        /// Write left blank
        writeString(String(row_number_width, ' '), out);
    }
    writeString(bottom_separator_s, out);

    total_rows += num_rows;
}


void PrettyBlockOutputFormat::writeValueWithPadding(
    const IColumn & column, const ISerialization & serialization, size_t row_num,
    size_t value_width, size_t pad_to_width, bool align_right)
{
    String serialized_value = " ";
    {
        WriteBufferFromString out_serialize(serialized_value, AppendModeTag());
        serialization.serializeText(column, row_num, out_serialize, format_settings);
    }

    if (value_width > format_settings.pretty.max_value_width)
    {
        serialized_value.resize(UTF8::computeBytesBeforeWidth(
            reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size(), 0, 1 + format_settings.pretty.max_value_width));

        const char * ellipsis = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8 ? "⋯" : "~";
        if (format_settings.pretty.color)
        {
            serialized_value += "\033[31;1m";
            serialized_value += ellipsis;
            serialized_value += "\033[0m";
        }
        else
            serialized_value += ellipsis;

        value_width = format_settings.pretty.max_value_width;
    }
    else
        serialized_value += ' ';

    auto write_padding = [&]()
    {
        if (pad_to_width > value_width)
            for (size_t k = 0; k < pad_to_width - value_width; ++k)
                writeChar(' ', out);
    };

    if (align_right)
    {
        write_padding();
        out.write(serialized_value.data(), serialized_value.size());
    }
    else
    {
        out.write(serialized_value.data(), serialized_value.size());
        write_padding();
    }
}


void PrettyBlockOutputFormat::consume(Chunk chunk)
{
    write(std::move(chunk), PortKind::Main);
}

void PrettyBlockOutputFormat::consumeTotals(Chunk chunk)
{
    total_rows = 0;
    writeCString("\nTotals:\n", out);
    write(std::move(chunk), PortKind::Totals);
}

void PrettyBlockOutputFormat::consumeExtremes(Chunk chunk)
{
    total_rows = 0;
    writeCString("\nExtremes:\n", out);
    write(std::move(chunk), PortKind::Extremes);
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


void registerOutputFormatPretty(FormatFactory & factory)
{
    factory.registerOutputFormat("Pretty", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams &,
        const FormatSettings & format_settings)
    {
        return std::make_shared<PrettyBlockOutputFormat>(buf, sample, format_settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("Pretty");

    factory.registerOutputFormat("PrettyNoEscapes", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams &,
        const FormatSettings & format_settings)
    {
        FormatSettings changed_settings = format_settings;
        changed_settings.pretty.color = false;
        return std::make_shared<PrettyBlockOutputFormat>(buf, sample, changed_settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("PrettyNoEscapes");
}

}
