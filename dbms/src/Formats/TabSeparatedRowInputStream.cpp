#include <IO/ReadHelpers.h>
#include <IO/Operators.h>

#include <Formats/TabSeparatedRowInputStream.h>
#include <Formats/verbosePrintString.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <DataTypes/DataTypeNothing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


static void skipTSVRow(ReadBuffer & istr, const size_t num_columns)
{
    NullSink null_sink;

    for (size_t i = 0; i < num_columns; ++i)
    {
        readEscapedStringInto(null_sink, istr);
        assertChar(i == num_columns - 1 ? '\n' : '\t', istr);
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


TabSeparatedRowInputStream::TabSeparatedRowInputStream(
    ReadBuffer & istr_, const Block & header_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : RowInputStreamWithDiagnosticInfo(istr_, header_), with_names(with_names_), with_types(with_types_), format_settings(format_settings_)
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


void TabSeparatedRowInputStream::setupAllColumnsByTableSchema()
{
    read_columns.assign(header.columns(), true);
    column_indexes_for_input_fields.resize(header.columns());

    for (size_t i = 0; i < column_indexes_for_input_fields.size(); ++i)
        column_indexes_for_input_fields[i] = i;
}


void TabSeparatedRowInputStream::addInputColumn(const String & column_name)
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
            "Unknown field found in TSV header: '" + column_name + "' " +
            "at position " + std::to_string(column_indexes_for_input_fields.size()) +
            "\nSet the 'input_format_skip_unknown_fields' parameter explicitly to ignore and proceed",
            ErrorCodes::INCORRECT_DATA
        );
    }

    const auto column_index = column_it->second;

    if (read_columns[column_index])
        throw Exception("Duplicate field found while parsing TSV header: " + column_name, ErrorCodes::INCORRECT_DATA);

    read_columns[column_index] = true;
    column_indexes_for_input_fields.emplace_back(column_index);
}


void TabSeparatedRowInputStream::fillUnreadColumnsWithDefaults(MutableColumns & columns, RowReadExtension & row_read_extension)
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


void TabSeparatedRowInputStream::readPrefix()
{
    if (with_names || with_types)
    {
        /// In this format, we assume that column name or type cannot contain BOM,
        ///  so, if format has header,
        ///  then BOM at beginning of stream cannot be confused with name or type of field, and it is safe to skip it.
        skipBOMIfExists(istr);
    }

    if (with_names)
    {
        if (format_settings.with_names_use_header)
        {
            String column_name;
            do
            {
                readEscapedString(column_name, istr);
                addInputColumn(column_name);
            }
            while (checkChar('\t', istr));

            if (!istr.eof())
            {
                checkForCarriageReturn(istr);
                assertChar('\n', istr);
            }
        }
        else
        {
            setupAllColumnsByTableSchema();
            skipTSVRow(istr, column_indexes_for_input_fields.size());
        }
    }
    else
        setupAllColumnsByTableSchema();

    if (with_types)
    {
        skipTSVRow(istr, column_indexes_for_input_fields.size());
    }
}


bool TabSeparatedRowInputStream::read(MutableColumns & columns, RowReadExtension & ext)
{
    if (istr.eof())
        return false;

    updateDiagnosticInfo();

    for (size_t input_position = 0; input_position < column_indexes_for_input_fields.size(); ++input_position)
    {
        const auto & column_index = column_indexes_for_input_fields[input_position];
        if (column_index)
        {
            data_types[*column_index]->deserializeAsTextEscaped(*columns[*column_index], istr, format_settings);
        }
        else
        {
            NullSink null_sink;
            readEscapedStringInto(null_sink, istr);
        }

        /// skip separators
        if (input_position + 1 < column_indexes_for_input_fields.size())
        {
            assertChar('\t', istr);
        }
        else if (!istr.eof())
        {
            if (unlikely(row_num == 1))
                checkForCarriageReturn(istr);

            assertChar('\n', istr);
        }
    }

    fillUnreadColumnsWithDefaults(columns, ext);

    return true;
}

bool TabSeparatedRowInputStream::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
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

void TabSeparatedRowInputStream::tryDeserializeFiled(const DataTypePtr & type, IColumn & column, size_t input_position,
                                                     ReadBuffer::Position & prev_pos,
                                                     ReadBuffer::Position & curr_pos)
{
    prev_pos = istr.position();
    if (column_indexes_for_input_fields[input_position])
        type->deserializeAsTextEscaped(column, istr, format_settings);
    else
    {
        NullSink null_sink;
        readEscapedStringInto(null_sink, istr);
    }
    curr_pos = istr.position();
}


void registerInputFormatTabSeparated(FormatFactory & factory)
{
    for (auto name : {"TabSeparated", "TSV"})
    {
        factory.registerInputFormat(name, [](
            ReadBuffer & buf,
            const Block & sample,
            const Context &,
            UInt64 max_block_size,
            UInt64 rows_portion_size,
            FormatFactory::ReadCallback callback,
            const FormatSettings & settings)
        {
            return std::make_shared<BlockInputStreamFromRowInputStream>(
                std::make_shared<TabSeparatedRowInputStream>(buf, sample, false, false, settings),
                sample, max_block_size, rows_portion_size, callback, settings);
        });
    }

    for (auto name : {"TabSeparatedWithNames", "TSVWithNames"})
    {
        factory.registerInputFormat(name, [](
            ReadBuffer & buf,
            const Block & sample,
            const Context &,
            UInt64 max_block_size,
            UInt64 rows_portion_size,
            FormatFactory::ReadCallback callback,
            const FormatSettings & settings)
        {
            return std::make_shared<BlockInputStreamFromRowInputStream>(
                std::make_shared<TabSeparatedRowInputStream>(buf, sample, true, false, settings),
                sample, max_block_size, rows_portion_size, callback, settings);
        });
    }

    for (auto name : {"TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes"})
    {
        factory.registerInputFormat(name, [](
            ReadBuffer & buf,
            const Block & sample,
            const Context &,
            UInt64 max_block_size,
            UInt64 rows_portion_size,
            FormatFactory::ReadCallback callback,
            const FormatSettings & settings)
        {
            return std::make_shared<BlockInputStreamFromRowInputStream>(
                std::make_shared<TabSeparatedRowInputStream>(buf, sample, true, true, settings),
                sample, max_block_size, rows_portion_size, callback, settings);
        });
    }
}

}
