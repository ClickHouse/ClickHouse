#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataStreams/TabSeparatedRowInputStream.h>
#include <DataStreams/verbosePrintString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}


TabSeparatedRowInputStream::TabSeparatedRowInputStream(ReadBuffer & istr_, const Block & header_, bool with_names_, bool with_types_)
    : istr(istr_), header(header_), with_names(with_names_), with_types(with_types_)
{
    size_t num_columns = header.columns();
    data_types.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        data_types[i] = header.safeGetByPosition(i).type;
}


void TabSeparatedRowInputStream::readPrefix()
{
    size_t num_columns = header.columns();
    String tmp;

    if (with_names || with_types)
    {
        /// In this format, we assume that column name or type cannot contain BOM,
        ///  so, if format has header,
        ///  then BOM at beginning of stream cannot be confused with name or type of field, and it is safe to skip it.
        skipBOMIfExists(istr);
    }

    if (with_names)
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            readEscapedString(tmp, istr);
            assertChar(i == num_columns - 1 ? '\n' : '\t', istr);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            readEscapedString(tmp, istr);
            assertChar(i == num_columns - 1 ? '\n' : '\t', istr);
        }
    }
}


/** Check for a common error case - usage of Windows line feed.
  */
static void checkForCarriageReturn(ReadBuffer & istr)
{
    if (istr.position()[0] == '\r' || (istr.position() != istr.buffer().begin() && istr.position()[-1] == '\r'))
        throw Exception("\nYou have carriage return (\\r, 0x0D, ASCII 13) at end of first row."
            "\nIt's like your input data has DOS/Windows style line separators, that are illegal in TabSeparated format."
            " You must transform your file to Unix format."
            "\nBut if you really need carriage return at end of string value of last column, you need to escape it as \\r.",
            ErrorCodes::INCORRECT_DATA);
}


bool TabSeparatedRowInputStream::read(MutableColumns & columns)
{
    if (istr.eof())
        return false;

    updateDiagnosticInfo();

    size_t size = data_types.size();

    for (size_t i = 0; i < size; ++i)
    {
        data_types[i]->deserializeTextEscaped(*columns[i], istr);

        /// skip separators
        if (i + 1 == size)
        {
            if (!istr.eof())
            {
                if (unlikely(row_num == 1))
                    checkForCarriageReturn(istr);

                assertChar('\n', istr);
            }
        }
        else
            assertChar('\t', istr);
    }

    return true;
}


String TabSeparatedRowInputStream::getDiagnosticInfo()
{
    if (istr.eof())        /// Buffer has gone, cannot extract information about what has been parsed.
        return {};

    WriteBufferFromOwnString out;
    MutableColumns columns = header.cloneEmptyColumns();

    /// It is possible to display detailed diagnostics only if the last and next to last lines are still in the read buffer.
    size_t bytes_read_at_start_of_buffer = istr.count() - istr.offset();
    if (bytes_read_at_start_of_buffer != bytes_read_at_start_of_buffer_on_prev_row)
    {
        out << "Could not print diagnostic info because two last rows aren't in buffer (rare case)\n";
        return out.str();
    }

    size_t max_length_of_column_name = 0;
    for (size_t i = 0; i < header.columns(); ++i)
        if (header.safeGetByPosition(i).name.size() > max_length_of_column_name)
            max_length_of_column_name = header.safeGetByPosition(i).name.size();

    size_t max_length_of_data_type_name = 0;
    for (size_t i = 0; i < header.columns(); ++i)
        if (header.safeGetByPosition(i).type->getName().size() > max_length_of_data_type_name)
            max_length_of_data_type_name = header.safeGetByPosition(i).type->getName().size();

    /// Roll back the cursor to the beginning of the previous or current line and pars all over again. But now we derive detailed information.

    if (pos_of_prev_row)
    {
        istr.position() = pos_of_prev_row;

        out << "\nRow " << (row_num - 1) << ":\n";
        if (!parseRowAndPrintDiagnosticInfo(columns, out, max_length_of_column_name, max_length_of_data_type_name))
            return out.str();
    }
    else
    {
        if (!pos_of_current_row)
        {
            out << "Could not print diagnostic info because parsing of data hasn't started.\n";
            return out.str();
        }

        istr.position() = pos_of_current_row;
    }

    out << "\nRow " << row_num << ":\n";
    parseRowAndPrintDiagnosticInfo(columns, out, max_length_of_column_name, max_length_of_data_type_name);
    out << "\n";

    return out.str();
}


bool TabSeparatedRowInputStream::parseRowAndPrintDiagnosticInfo(MutableColumns & columns,
    WriteBuffer & out, size_t max_length_of_column_name, size_t max_length_of_data_type_name)
{
    size_t size = data_types.size();
    for (size_t i = 0; i < size; ++i)
    {
        if (i == 0 && istr.eof())
        {
            out << "<End of stream>\n";
            return false;
        }

        out << "Column " << i << ", " << std::string((i < 10 ? 2 : i < 100 ? 1 : 0), ' ')
            << "name: " << header.safeGetByPosition(i).name << ", " << std::string(max_length_of_column_name - header.safeGetByPosition(i).name.size(), ' ')
            << "type: " << data_types[i]->getName() << ", " << std::string(max_length_of_data_type_name - data_types[i]->getName().size(), ' ');

        auto prev_position = istr.position();
        std::exception_ptr exception;

        try
        {
            data_types[i]->deserializeTextEscaped(*columns[i], istr);
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        auto curr_position = istr.position();

        if (curr_position < prev_position)
            throw Exception("Logical error: parsing is non-deterministic.", ErrorCodes::LOGICAL_ERROR);

        if (data_types[i]->isNumber() || data_types[i]->isDateOrDateTime())
        {
            /// An empty string instead of a value.
            if (curr_position == prev_position)
            {
                out << "ERROR: text ";
                verbosePrintString(prev_position, std::min(prev_position + 10, istr.buffer().end()), out);
                out << " is not like " << data_types[i]->getName() << "\n";
                return false;
            }
        }

        out << "parsed text: ";
        verbosePrintString(prev_position, curr_position, out);

        if (exception)
        {
            if (data_types[i]->getName() == "DateTime")
                out << "ERROR: DateTime must be in YYYY-MM-DD hh:mm:ss or NNNNNNNNNN (unix timestamp, exactly 10 digits) format.\n";
            else if (data_types[i]->getName() == "Date")
                out << "ERROR: Date must be in YYYY-MM-DD format.\n";
            else
                out << "ERROR\n";
            return false;
        }

        out << "\n";

        if (data_types[i]->haveMaximumSizeOfValue())
        {
            if (*curr_position != '\n' && *curr_position != '\t')
            {
                out << "ERROR: garbage after " << data_types[i]->getName() << ": ";
                verbosePrintString(curr_position, std::min(curr_position + 10, istr.buffer().end()), out);
                out << "\n";

                if (data_types[i]->getName() == "DateTime")
                    out << "ERROR: DateTime must be in YYYY-MM-DD hh:mm:ss or NNNNNNNNNN (unix timestamp, exactly 10 digits) format.\n";
                else if (data_types[i]->getName() == "Date")
                    out << "ERROR: Date must be in YYYY-MM-DD format.\n";

                return false;
            }
        }

        /// Delimiters
        if (i + 1 == size)
        {
            if (!istr.eof())
            {
                try
                {
                    assertChar('\n', istr);
                }
                catch (const DB::Exception &)
                {
                    if (*istr.position() == '\t')
                    {
                        out << "ERROR: Tab found where line feed is expected."
                            " It's like your file has more columns than expected.\n"
                            "And if your file have right number of columns, maybe it have unescaped tab in value.\n";
                    }
                    else if (*istr.position() == '\r')
                    {
                        out << "ERROR: Carriage return found where line feed is expected."
                            " It's like your file has DOS/Windows style line separators, that is illegal in TabSeparated format.\n";
                    }
                    else
                    {
                        out << "ERROR: There is no line feed. ";
                        verbosePrintString(istr.position(), istr.position() + 1, out);
                        out << " found instead.\n";
                    }
                    return false;
                }
            }
        }
        else
        {
            try
            {
                assertChar('\t', istr);
            }
            catch (const DB::Exception &)
            {
                if (*istr.position() == '\n')
                {
                    out << "ERROR: Line feed found where tab is expected."
                        " It's like your file has less columns than expected.\n"
                        "And if your file have right number of columns, maybe it have unescaped backslash in value before tab, which cause tab has escaped.\n";
                }
                else if (*istr.position() == '\r')
                {
                    out << "ERROR: Carriage return found where tab is expected.\n";
                }
                else
                {
                    out << "ERROR: There is no tab. ";
                    verbosePrintString(istr.position(), istr.position() + 1, out);
                    out << " found instead.\n";
                }
                return false;
            }
        }
    }

    return true;
}


void TabSeparatedRowInputStream::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(istr);
}


void TabSeparatedRowInputStream::updateDiagnosticInfo()
{
    ++row_num;

    bytes_read_at_start_of_buffer_on_prev_row = bytes_read_at_start_of_buffer_on_current_row;
    bytes_read_at_start_of_buffer_on_current_row = istr.count() - istr.offset();

    pos_of_prev_row = pos_of_current_row;
    pos_of_current_row = istr.position();
}

}
