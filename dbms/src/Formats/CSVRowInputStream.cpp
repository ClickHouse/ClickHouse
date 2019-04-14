#include <IO/ReadHelpers.h>
#include <IO/Operators.h>

#include <Formats/verbosePrintString.h>
#include <Formats/CSVRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <DataTypes/DataTypeNothing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


static inline void skipEndOfLine(ReadBuffer & istr)
{
    /// \n (Unix) or \r\n (DOS/Windows) or \n\r (Mac OS Classic)

    if (*istr.position() == '\n')
    {
        ++istr.position();
        if (!istr.eof() && *istr.position() == '\r')
            ++istr.position();
    }
    else if (*istr.position() == '\r')
    {
        ++istr.position();
        if (!istr.eof() && *istr.position() == '\n')
            ++istr.position();
        else
            throw Exception("Cannot parse CSV format: found \\r (CR) not followed by \\n (LF)."
                " Line must end by \\n (LF) or \\r\\n (CR LF) or \\n\\r.", ErrorCodes::INCORRECT_DATA);
    }
    else if (!istr.eof())
        throw Exception("Expected end of line", ErrorCodes::INCORRECT_DATA);
}


static inline void skipDelimiter(ReadBuffer & istr, const char delimiter, bool is_last_column)
{
    if (is_last_column)
    {
        if (istr.eof())
            return;

        /// we support the extra delimiter at the end of the line
        if (*istr.position() == delimiter)
        {
            ++istr.position();
            if (istr.eof())
                return;
        }

        skipEndOfLine(istr);
    }
    else
        assertChar(delimiter, istr);
}


/// Skip `whitespace` symbols allowed in CSV.
static inline void skipWhitespacesAndTabs(ReadBuffer & buf)
{
    while (!buf.eof()
            && (*buf.position() == ' '
                || *buf.position() == '\t'))
        ++buf.position();
}


static void skipRow(ReadBuffer & istr, const FormatSettings::CSV & settings, size_t num_columns)
{
    String tmp;
    for (size_t i = 0; i < num_columns; ++i)
    {
        skipWhitespacesAndTabs(istr);
        readCSVString(tmp, istr, settings);
        skipWhitespacesAndTabs(istr);

        skipDelimiter(istr, settings.delimiter, i + 1 == num_columns);
    }
}


CSVRowInputStream::CSVRowInputStream(ReadBuffer & istr_, const Block & header_, bool with_names_, const FormatSettings & format_settings)
        : RowInputStreamWithDiagnosticInfo(istr_, header_), with_names(with_names_), format_settings(format_settings)
{
    const auto num_columns = header.columns();

    data_types.resize(num_columns);
    column_indexes_by_names.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column_info = header.getByPosition(i);

        data_types[i] = column_info.type;
        column_indexes_by_names.emplace(column_info.name, i);
    }

    column_indexes_for_input_fields.reserve(num_columns);
    read_columns.assign(num_columns, false);
}


void CSVRowInputStream::setupAllColumnsByTableSchema()
{
    read_columns.assign(header.columns(), true);
    column_indexes_for_input_fields.resize(header.columns());

    for (size_t i = 0; i < column_indexes_for_input_fields.size(); ++i)
    {
        column_indexes_for_input_fields[i] = i;
    }
}


void CSVRowInputStream::addInputColumn(const String & column_name)
{
    const auto column_it = column_indexes_by_names.find(column_name);
    if (column_it == column_indexes_by_names.end())
    {
        if (format_settings.skip_unknown_fields)
        {
            column_indexes_for_input_fields.push_back(std::nullopt);
            return;
        }

        throw Exception(
            "Unknown field found in CSV header: '" + column_name + "' " +
            "at position " + std::to_string(column_indexes_for_input_fields.size()) +
            "\nSet the 'input_format_skip_unknown_fields' parameter explicitly to ignore and proceed",
            ErrorCodes::INCORRECT_DATA
        );
    }

    const auto column_index = column_it->second;

    if (read_columns[column_index])
        throw Exception("Duplicate field found while parsing CSV header: " + column_name, ErrorCodes::INCORRECT_DATA);

    read_columns[column_index] = true;
    column_indexes_for_input_fields.emplace_back(column_index);
}


void CSVRowInputStream::fillUnreadColumnsWithDefaults(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    /// It is safe to memorize this on the first run - the format guarantees this does not change
    if (unlikely(row_num == 1))
    {
        columns_to_fill_with_default_values.clear();
        for (size_t index = 0; index < read_columns.size(); ++index)
            if (read_columns[index] == 0)
                columns_to_fill_with_default_values.push_back(index);
    }

    for (const auto column_index : columns_to_fill_with_default_values)
        data_types[column_index]->insertDefaultInto(*columns[column_index]);

    row_read_extension.read_columns = read_columns;
}


void CSVRowInputStream::readPrefix()
{
    /// In this format, we assume, that if first string field contain BOM as value, it will be written in quotes,
    ///  so BOM at beginning of stream cannot be confused with BOM in first string value, and it is safe to skip it.
    skipBOMIfExists(istr);

    if (with_names)
    {
        if (format_settings.with_names_use_header)
        {
            String column_name;
            do
            {
                skipWhitespacesAndTabs(istr);
                readCSVString(column_name, istr, format_settings.csv);
                skipWhitespacesAndTabs(istr);

                addInputColumn(column_name);
            }
            while (checkChar(format_settings.csv.delimiter, istr));

            skipDelimiter(istr, format_settings.csv.delimiter, true);
        }
        else
        {
            setupAllColumnsByTableSchema();
            skipRow(istr, format_settings.csv, column_indexes_for_input_fields.size());
        }
    }
    else
    {
        setupAllColumnsByTableSchema();
    }
}


bool CSVRowInputStream::read(MutableColumns & columns, RowReadExtension & ext)
{
    if (istr.eof())
        return false;

    updateDiagnosticInfo();

    String tmp;
    for (size_t input_position = 0; input_position < column_indexes_for_input_fields.size(); ++input_position)
    {
        const auto & column_index = column_indexes_for_input_fields[input_position];
        if (column_index)
        {
            skipWhitespacesAndTabs(istr);
            data_types[*column_index]->deserializeAsTextCSV(*columns[*column_index], istr, format_settings);
            skipWhitespacesAndTabs(istr);
        }
        else
        {
            readCSVString(tmp, istr, format_settings.csv);
        }

        skipDelimiter(istr, format_settings.csv.delimiter, input_position + 1 == column_indexes_for_input_fields.size());
    }

    fillUnreadColumnsWithDefaults(columns, ext);

    return true;
}

bool CSVRowInputStream::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    const char delimiter = format_settings.csv.delimiter;

    for (size_t input_position = 0; input_position < column_indexes_for_input_fields.size(); ++input_position)
    {
        if (input_position == 0 && istr.eof())
        {
            out << "<End of stream>\n";
            return false;
        }

        if (column_indexes_for_input_fields[input_position].has_value())
        {
            size_t col_idx = column_indexes_for_input_fields[input_position].value();
            if (!deserializeFieldAndPrintDiagnosticInfo(header.getByPosition(col_idx).name, data_types[col_idx], *columns[col_idx],
                                                        out, input_position))
                return false;
        }
        else
        {
            static const String skipped_column_str = "<SKIPPED COLUMN>";
            static const DataTypePtr skipped_column_type = std::make_shared<DataTypeNothing>();
            static const MutableColumnPtr skipped_column = skipped_column_type->createColumn();
            if (!deserializeFieldAndPrintDiagnosticInfo(skipped_column_str, skipped_column_type, *skipped_column, out, input_position))
                return false;
        }

        /// Delimiters
        if (input_position + 1 == column_indexes_for_input_fields.size())
        {
            if (istr.eof())
                return false;

            /// we support the extra delimiter at the end of the line
            if (*istr.position() == delimiter)
            {
                ++istr.position();
                if (istr.eof())
                    break;
            }

            if (!istr.eof() && *istr.position() != '\n' && *istr.position() != '\r')
            {
                out << "ERROR: There is no line feed. ";
                verbosePrintString(istr.position(), istr.position() + 1, out);
                out << " found instead.\n"
                    " It's like your file has more columns than expected.\n"
                    "And if your file have right number of columns, maybe it have unquoted string value with comma.\n";

                return false;
            }

            skipEndOfLine(istr);
        }
        else
        {
            try
            {
                assertChar(delimiter, istr);
            }
            catch (const DB::Exception &)
            {
                if (*istr.position() == '\n' || *istr.position() == '\r')
                {
                    out << "ERROR: Line feed found where delimiter (" << delimiter << ") is expected."
                        " It's like your file has less columns than expected.\n"
                        "And if your file have right number of columns, maybe it have unescaped quotes in values.\n";
                }
                else
                {
                    out << "ERROR: There is no delimiter (" << delimiter << "). ";
                    verbosePrintString(istr.position(), istr.position() + 1, out);
                    out << " found instead.\n";
                }
                return false;
            }
        }
    }

    return true;
}


void CSVRowInputStream::syncAfterError()
{
    skipToNextLineOrEOF(istr);
}

void
CSVRowInputStream::tryDeserializeFiled(const DataTypePtr & type, IColumn & column, size_t input_position, ReadBuffer::Position & prev_pos,
                                       ReadBuffer::Position & curr_pos)
{
    skipWhitespacesAndTabs(istr);
    prev_pos = istr.position();
    if (column_indexes_for_input_fields[input_position])
        type->deserializeAsTextCSV(column, istr, format_settings);
    else
    {
        String tmp;
        readCSVString(tmp, istr, format_settings.csv);
    }
    curr_pos = istr.position();
    skipWhitespacesAndTabs(istr);
}


void registerInputFormatCSV(FormatFactory & factory)
{
    for (bool with_names : {false, true})
    {
        factory.registerInputFormat(with_names ? "CSVWithNames" : "CSV", [=](
            ReadBuffer & buf,
            const Block & sample,
            const Context &,
            UInt64 max_block_size,
            const FormatSettings & settings)
        {
            return std::make_shared<BlockInputStreamFromRowInputStream>(
                std::make_shared<CSVRowInputStream>(buf, sample, with_names, settings),
                sample, max_block_size, settings);
        });
    }
}

}
