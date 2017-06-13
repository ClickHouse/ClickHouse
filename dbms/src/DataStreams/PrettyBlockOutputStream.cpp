#include <sys/ioctl.h>
#include <unistd.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/PrettyBlockOutputStream.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


PrettyBlockOutputStream::PrettyBlockOutputStream(WriteBuffer & ostr_, bool no_escapes_, size_t max_rows_, const Context & context_)
     : ostr(ostr_), max_rows(max_rows_), no_escapes(no_escapes_), context(context_)
{
    struct winsize w;
    if (0 == ioctl(STDOUT_FILENO, TIOCGWINSZ, &w))
        terminal_width = w.ws_col;
}


void PrettyBlockOutputStream::flush()
{
    ostr.next();
}


void PrettyBlockOutputStream::calculateWidths(Block & block, Widths_t & max_widths, Widths_t & name_widths)
{
    size_t rows = block.rows();
    size_t columns = block.columns();

    max_widths.resize(columns);
    name_widths.resize(columns);

    FunctionPtr visible_width_func = FunctionFactory::instance().get("visibleWidth", context);

    /// Calculate widths of all values.
    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName column;
        column.type = std::make_shared<DataTypeUInt64>();
        column.name = "visibleWidth(" + block.getByPosition(i).name + ")";

        size_t result_number = block.columns();
        block.insert(column);

        visible_width_func->execute(block, {i}, result_number);
        column.column = block.safeGetByPosition(result_number).column;

        if (const ColumnUInt64 * col = typeid_cast<const ColumnUInt64 *>(&*column.column))
        {
            const ColumnUInt64::Container_t & res = col->getData();
            for (size_t j = 0; j < rows; ++j)
            {
                if (res[j] > max_widths[i])
                    max_widths[i] = res[j];
            }
        }
        else if (const ColumnConstUInt64 * col = typeid_cast<const ColumnConstUInt64 *>(&*column.column))
        {
            UInt64 res = col->getData();
            max_widths[i] = res;
        }
        else
            throw Exception("Illegal column " + column.column->getName()
                + " of result of function " + visible_width_func->getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        /// And also calculate widths for names of columns.
        {
            const String & name = block.getByPosition(i).name;

            Block block_with_name
            {
                { std::make_shared<ColumnConstString>(1, name), std::make_shared<DataTypeString>(), "name" },
                { nullptr, std::make_shared<DataTypeUInt64>(), "width" }
            };

            visible_width_func->execute(block_with_name, {0}, 1);

            name_widths[i] = typeid_cast<const ColumnConstUInt64 &>(*block_with_name.getByPosition(1).column).getData();

            if (name_widths[i] > max_widths[i])
                max_widths[i] = name_widths[i];
        }
    }
}


void PrettyBlockOutputStream::write(const Block & block_)
{
    if (total_rows >= max_rows)
    {
        total_rows += block_.rows();
        return;
    }

    /// We will insert here columns with the calculated values of visible lengths.
    Block block = block_;

    size_t rows = block.rows();
    size_t columns = block.columns();

    Widths_t max_widths;
    Widths_t name_widths;
    calculateWidths(block, max_widths, name_widths);

    /// Create separators
    std::stringstream top_separator;
    std::stringstream middle_names_separator;
    std::stringstream middle_values_separator;
    std::stringstream bottom_separator;

    top_separator             << "┏";
    middle_names_separator    << "┡";
    middle_values_separator    << "├";
    bottom_separator         << "└";
    for (size_t i = 0; i < columns; ++i)
    {
        if (i != 0)
        {
            top_separator             << "┳";
            middle_names_separator    << "╇";
            middle_values_separator    << "┼";
            bottom_separator         << "┴";
        }

        for (size_t j = 0; j < max_widths[i] + 2; ++j)
        {
            top_separator             << "━";
            middle_names_separator    << "━";
            middle_values_separator    << "─";
            bottom_separator         << "─";
        }
    }
    top_separator             << "┓\n";
    middle_names_separator    << "┩\n";
    middle_values_separator    << "┤\n";
    bottom_separator         << "┘\n";

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

        const ColumnWithTypeAndName & col = block.safeGetByPosition(i);

        if (!no_escapes)
            writeCString("\033[1m", ostr);

        if (col.type->isNumeric())
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

            const ColumnWithTypeAndName & col = block.safeGetByPosition(j);

            if (col.type->isNumeric())
            {
                size_t width = get<UInt64>((*block.safeGetByPosition(columns + j).column)[i]);
                for (size_t k = 0; k < max_widths[j] - width; ++k)
                    writeChar(' ', ostr);

                col.type->serializeTextEscaped(*col.column.get(), i, ostr);
            }
            else
            {
                col.type->serializeTextEscaped(*col.column.get(), i, ostr);

                size_t width = get<UInt64>((*block.safeGetByPosition(columns + j).column)[i]);
                for (size_t k = 0; k < max_widths[j] - width; ++k)
                    writeChar(' ', ostr);
            }
        }

        writeCString(" │\n", ostr);
    }

    writeString(bottom_separator_s, ostr);

    total_rows += rows;
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
