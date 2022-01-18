#include <Processors/Formats/RowInputFormatWithDiagnosticInfo.h>
#include <Formats/verbosePrintString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static String alignedName(const String & name, size_t max_length)
{
    size_t spaces_count = max_length >= name.size() ? max_length - name.size() : 0;
    return name + ", " + std::string(spaces_count, ' ');
}


RowInputFormatWithDiagnosticInfo::RowInputFormatWithDiagnosticInfo(const Block & header_, ReadBuffer & in_, const Params & params_)
    : IRowInputFormat(header_, in_, params_)
{
}

void RowInputFormatWithDiagnosticInfo::updateDiagnosticInfo()
{
    ++row_num;

    bytes_read_at_start_of_buffer_on_prev_row = bytes_read_at_start_of_buffer_on_current_row;
    bytes_read_at_start_of_buffer_on_current_row = in.count() - in.offset();

    offset_of_prev_row = offset_of_current_row;
    offset_of_current_row = in.offset();
}

String RowInputFormatWithDiagnosticInfo::getDiagnosticInfo()
{
    if (in.eof())
        return "Buffer has gone, cannot extract information about what has been parsed.";

    WriteBufferFromOwnString out;

    const auto & header = getPort().getHeader();
    MutableColumns columns = header.cloneEmptyColumns();

    /// It is possible to display detailed diagnostics only if the last and next to last rows are still in the read buffer.
    size_t bytes_read_at_start_of_buffer = in.count() - in.offset();
    if (bytes_read_at_start_of_buffer != bytes_read_at_start_of_buffer_on_prev_row)
    {
        out << "Could not print diagnostic info because two last rows aren't in buffer (rare case)\n";
        return out.str();
    }

    max_length_of_column_name = 0;
    for (size_t i = 0; i < header.columns(); ++i)
        if (header.safeGetByPosition(i).name.size() > max_length_of_column_name)
            max_length_of_column_name = header.safeGetByPosition(i).name.size();

    max_length_of_data_type_name = 0;
    for (size_t i = 0; i < header.columns(); ++i)
        if (header.safeGetByPosition(i).type->getName().size() > max_length_of_data_type_name)
            max_length_of_data_type_name = header.safeGetByPosition(i).type->getName().size();

    /// Roll back the cursor to the beginning of the previous or current row and parse all over again. But now we derive detailed information.

    if (offset_of_prev_row <= in.buffer().size())
    {
        in.position() = in.buffer().begin() + offset_of_prev_row;

        out << "\nRow " << (row_num - 1) << ":\n";
        if (!parseRowAndPrintDiagnosticInfo(columns, out))
            return out.str();
    }
    else
    {
        if (in.buffer().size() < offset_of_current_row)
        {
            out << "Could not print diagnostic info because parsing of data hasn't started.\n";
            return out.str();
        }

        in.position() = in.buffer().begin() + offset_of_current_row;
    }

    out << "\nRow " << row_num << ":\n";
    parseRowAndPrintDiagnosticInfo(columns, out);
    out << "\n";

    return out.str();
}

bool RowInputFormatWithDiagnosticInfo::deserializeFieldAndPrintDiagnosticInfo(const String & col_name,
                                                                              const DataTypePtr & type,
                                                                              IColumn & column,
                                                                              WriteBuffer & out,
                                                                              size_t file_column)
{
    out << "Column " << file_column << ", " << std::string((file_column < 10 ? 2 : file_column < 100 ? 1 : 0), ' ')
        << "name: " << alignedName(col_name, max_length_of_column_name)
        << "type: " << alignedName(type->getName(), max_length_of_data_type_name);

    auto * prev_position = in.position();
    std::exception_ptr exception;

    try
    {
        tryDeserializeField(type, column, file_column);
    }
    catch (...)
    {
        exception = std::current_exception();
    }
    auto * curr_position = in.position();

    if (curr_position < prev_position)
        throw Exception("Logical error: parsing is non-deterministic.", ErrorCodes::LOGICAL_ERROR);

    if (isNativeNumber(type) || isDate(type) || isDateTime(type) || isDateTime64(type))
    {
        /// An empty string instead of a value.
        if (curr_position == prev_position)
        {
            out << "ERROR: text ";
            verbosePrintString(prev_position, std::min(prev_position + 10, in.buffer().end()), out);
            out << " is not like " << type->getName() << "\n";
            return false;
        }
    }

    out << "parsed text: ";
    verbosePrintString(prev_position, curr_position, out);

    if (exception)
    {
        if (type->getName() == "DateTime")
            out << "ERROR: DateTime must be in YYYY-MM-DD hh:mm:ss or NNNNNNNNNN (unix timestamp, exactly 10 digits) format.\n";
        else if (type->getName() == "Date")
            out << "ERROR: Date must be in YYYY-MM-DD format.\n";
        else
            out << "ERROR\n";
        // Print exception message
        out << getExceptionMessage(exception, false) << '\n';
        return false;
    }

    out << "\n";

    if (type->haveMaximumSizeOfValue())
    {
        if (isGarbageAfterField(file_column, curr_position))
        {
            out << "ERROR: garbage after " << type->getName() << ": ";
            verbosePrintString(curr_position, std::min(curr_position + 10, in.buffer().end()), out);
            out << "\n";

            if (type->getName() == "DateTime")
                out << "ERROR: DateTime must be in YYYY-MM-DD hh:mm:ss or NNNNNNNNNN (unix timestamp, exactly 10 digits) format.\n";
            else if (type->getName() == "Date")
                out << "ERROR: Date must be in YYYY-MM-DD format.\n";

            return false;
        }
    }

    return true;
}

void RowInputFormatWithDiagnosticInfo::resetParser()
{
    IRowInputFormat::resetParser();
    row_num = 0;
    bytes_read_at_start_of_buffer_on_current_row = 0;
    bytes_read_at_start_of_buffer_on_prev_row = 0;
    offset_of_current_row = std::numeric_limits<size_t>::max();
    offset_of_prev_row = std::numeric_limits<size_t>::max();
    max_length_of_column_name = 0;
    max_length_of_data_type_name = 0;
}


}
