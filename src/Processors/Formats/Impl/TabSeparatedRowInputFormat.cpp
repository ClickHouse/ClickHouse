#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>
#include <Processors/Formats/Impl/TabSeparatedRawRowInputFormat.h>
#include <Formats/verbosePrintString.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


static void skipTSVRow(ReadBuffer & in, const size_t num_columns)
{
    NullOutput null_sink;

    for (size_t i = 0; i < num_columns; ++i)
    {
        readEscapedStringInto(null_sink, in);
        assertChar(i == num_columns - 1 ? '\n' : '\t', in);
    }
}


/** Check for a common error case - usage of Windows line feed.
  */
static void checkForCarriageReturn(ReadBuffer & in)
{
    if (in.position()[0] == '\r' || (in.position() != in.buffer().begin() && in.position()[-1] == '\r'))
        throw Exception("\nYou have carriage return (\\r, 0x0D, ASCII 13) at end of first row."
            "\nIt's like your input data has DOS/Windows style line separators, that are illegal in TabSeparated format."
            " You must transform your file to Unix format."
            "\nBut if you really need carriage return at end of string value of last column, you need to escape it as \\r.",
            ErrorCodes::INCORRECT_DATA);
}


TabSeparatedRowInputFormat::TabSeparatedRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                                                       bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : RowInputFormatWithDiagnosticInfo(header_, in_, params_), with_names(with_names_), with_types(with_types_), format_settings(format_settings_)
{
    const auto & sample = getPort().getHeader();
    size_t num_columns = sample.columns();

    data_types.resize(num_columns);
    column_indexes_by_names.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column_info = sample.getByPosition(i);

        data_types[i] = column_info.type;
        column_indexes_by_names.emplace(column_info.name, i);
    }

    column_indexes_for_input_fields.reserve(num_columns);
    read_columns.assign(num_columns, false);
}


void TabSeparatedRowInputFormat::setupAllColumnsByTableSchema()
{
    const auto & header = getPort().getHeader();
    read_columns.assign(header.columns(), true);
    column_indexes_for_input_fields.resize(header.columns());

    for (size_t i = 0; i < column_indexes_for_input_fields.size(); ++i)
        column_indexes_for_input_fields[i] = i;
}


void TabSeparatedRowInputFormat::addInputColumn(const String & column_name)
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


void TabSeparatedRowInputFormat::fillUnreadColumnsWithDefaults(MutableColumns & columns, RowReadExtension & row_read_extension)
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
    {
        data_types[column_index]->insertDefaultInto(*columns[column_index]);
        row_read_extension.read_columns[column_index] = false;
    }
}


void TabSeparatedRowInputFormat::readPrefix()
{
    if (with_names || with_types || data_types.at(0)->textCanContainOnlyValidUTF8())
    {
        /// In this format, we assume that column name or type cannot contain BOM,
        ///  so, if format has header,
        ///  then BOM at beginning of stream cannot be confused with name or type of field, and it is safe to skip it.
        skipBOMIfExists(in);
    }

    if (with_names)
    {
        if (format_settings.with_names_use_header)
        {
            String column_name;
            for (;;)
            {
                readEscapedString(column_name, in);
                if (!checkChar('\t', in))
                {
                    /// Check last column for \r before adding it, otherwise an error will be:
                    ///     "Unknown field found in TSV header"
                    checkForCarriageReturn(in);
                    addInputColumn(column_name);
                    break;
                }
                else
                    addInputColumn(column_name);
            }


            if (!in.eof())
            {
                assertChar('\n', in);
            }
        }
        else
        {
            setupAllColumnsByTableSchema();
            skipTSVRow(in, column_indexes_for_input_fields.size());
        }
    }
    else
        setupAllColumnsByTableSchema();

    if (with_types)
    {
        skipTSVRow(in, column_indexes_for_input_fields.size());
    }
}


bool TabSeparatedRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in.eof())
        return false;

    updateDiagnosticInfo();

    ext.read_columns.assign(read_columns.size(), true);
    for (size_t file_column = 0; file_column < column_indexes_for_input_fields.size(); ++file_column)
    {
        const auto & column_index = column_indexes_for_input_fields[file_column];
        const bool is_last_file_column = file_column + 1 == column_indexes_for_input_fields.size();
        if (column_index)
        {
            const auto & type = data_types[*column_index];
            ext.read_columns[*column_index] = readField(*columns[*column_index], type, is_last_file_column);
        }
        else
        {
            NullOutput null_sink;
            readEscapedStringInto(null_sink, in);
        }

        /// skip separators
        if (file_column + 1 < column_indexes_for_input_fields.size())
        {
            assertChar('\t', in);
        }
        else if (!in.eof())
        {
            if (unlikely(row_num == 1))
                checkForCarriageReturn(in);

            assertChar('\n', in);
        }
    }

    fillUnreadColumnsWithDefaults(columns, ext);

    return true;
}


bool TabSeparatedRowInputFormat::readField(IColumn & column, const DataTypePtr & type, bool is_last_file_column)
{
    const bool at_delimiter = !is_last_file_column && !in.eof() && *in.position() == '\t';
    const bool at_last_column_line_end = is_last_file_column && (in.eof() || *in.position() == '\n');
    if (format_settings.tsv.empty_as_default && (at_delimiter || at_last_column_line_end))
    {
        column.insertDefault();
        return false;
    }
    else if (format_settings.null_as_default && !type->isNullable())
        return DataTypeNullable::deserializeTextEscaped(column, in, format_settings, type);
    type->deserializeAsTextEscaped(column, in, format_settings);
    return true;
}

bool TabSeparatedRowInputFormat::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    for (size_t file_column = 0; file_column < column_indexes_for_input_fields.size(); ++file_column)
    {
        if (file_column == 0 && in.eof())
        {
            out << "<End of stream>\n";
            return false;
        }

        if (column_indexes_for_input_fields[file_column].has_value())
        {
            const auto & header = getPort().getHeader();
            size_t col_idx = column_indexes_for_input_fields[file_column].value();
            if (!deserializeFieldAndPrintDiagnosticInfo(header.getByPosition(col_idx).name, data_types[col_idx], *columns[col_idx],
                                                        out, file_column))
                return false;
        }
        else
        {
            static const String skipped_column_str = "<SKIPPED COLUMN>";
            static const DataTypePtr skipped_column_type = std::make_shared<DataTypeNothing>();
            static const MutableColumnPtr skipped_column = skipped_column_type->createColumn();
            if (!deserializeFieldAndPrintDiagnosticInfo(skipped_column_str, skipped_column_type, *skipped_column, out, file_column))
                return false;
        }

        /// Delimiters
        if (file_column + 1 == column_indexes_for_input_fields.size())
        {
            if (!in.eof())
            {
                try
                {
                    assertChar('\n', in);
                }
                catch (const DB::Exception &)
                {
                    if (*in.position() == '\t')
                    {
                        out << "ERROR: Tab found where line feed is expected."
                               " It's like your file has more columns than expected.\n"
                               "And if your file have right number of columns, maybe it have unescaped tab in value.\n";
                    }
                    else if (*in.position() == '\r')
                    {
                        out << "ERROR: Carriage return found where line feed is expected."
                               " It's like your file has DOS/Windows style line separators, that is illegal in TabSeparated format.\n";
                    }
                    else
                    {
                        out << "ERROR: There is no line feed. ";
                        verbosePrintString(in.position(), in.position() + 1, out);
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
                assertChar('\t', in);
            }
            catch (const DB::Exception &)
            {
                if (*in.position() == '\n')
                {
                    out << "ERROR: Line feed found where tab is expected."
                           " It's like your file has less columns than expected.\n"
                           "And if your file have right number of columns, "
                           "maybe it have unescaped backslash in value before tab, which cause tab has escaped.\n";
                }
                else if (*in.position() == '\r')
                {
                    out << "ERROR: Carriage return found where tab is expected.\n";
                }
                else
                {
                    out << "ERROR: There is no tab. ";
                    verbosePrintString(in.position(), in.position() + 1, out);
                    out << " found instead.\n";
                }
                return false;
            }
        }
    }

    return true;
}

void TabSeparatedRowInputFormat::tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column)
{
    if (column_indexes_for_input_fields[file_column])
    {
        // check null value for type is not nullable. don't cross buffer bound for simplicity, so maybe missing some case
        if (!type->isNullable() && !in.eof())
        {
            if (*in.position() == '\\' && in.available() >= 2)
            {
                ++in.position();
                if (*in.position() == 'N')
                {
                    ++in.position();
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected NULL value of not Nullable type {}", type->getName());
                }
                else
                {
                    --in.position();
                }
            }
        }
        const bool is_last_file_column = file_column + 1 == column_indexes_for_input_fields.size();
        readField(column, type, is_last_file_column);
    }
    else
    {
        NullOutput null_sink;
        readEscapedStringInto(null_sink, in);
    }
}

void TabSeparatedRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(in);
}

void TabSeparatedRowInputFormat::resetParser()
{
    RowInputFormatWithDiagnosticInfo::resetParser();
    const auto & sample = getPort().getHeader();
    read_columns.assign(sample.columns(), false);
    column_indexes_for_input_fields.clear();
    columns_to_fill_with_default_values.clear();
}

void registerInputFormatProcessorTabSeparated(FormatFactory & factory)
{
    for (const auto * name : {"TabSeparated", "TSV"})
    {
        factory.registerInputFormatProcessor(name, [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(sample, buf, params, false, false, settings);
        });
    }

    for (const auto * name : {"TabSeparatedRaw", "TSVRaw"})
    {
        factory.registerInputFormatProcessor(name, [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRawRowInputFormat>(sample, buf, params, false, false, settings);
        });
    }

    for (const auto * name : {"TabSeparatedWithNames", "TSVWithNames"})
    {
        factory.registerInputFormatProcessor(name, [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(sample, buf, params, true, false, settings);
        });
    }

    for (const auto * name : {"TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes"})
    {
        factory.registerInputFormatProcessor(name, [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(sample, buf, params, true, true, settings);
        });
    }
}

static bool fileSegmentationEngineTabSeparatedImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
{
    bool need_more_data = true;
    char * pos = in.position();

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        pos = find_first_symbols<'\\', '\r', '\n'>(pos, in.buffer().end());

        if (pos == in.buffer().end())
            continue;

        if (*pos == '\\')
        {
            ++pos;
            if (loadAtPosition(in, memory, pos))
                ++pos;
        }
        else if (*pos == '\n' || *pos == '\r')
        {
            if (memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size)
                need_more_data = false;
            ++pos;
        }
    }

    saveUpToPosition(in, memory, pos);

    return loadAtPosition(in, memory, pos);
}

void registerFileSegmentationEngineTabSeparated(FormatFactory & factory)
{
    // We can use the same segmentation engine for TSKV.
    for (const auto * name : {"TabSeparated", "TSV", "TSKV"})
    {
        factory.registerFileSegmentationEngine(name, &fileSegmentationEngineTabSeparatedImpl);
    }
}

}
