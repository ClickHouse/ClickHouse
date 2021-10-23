#include <IO/ReadHelpers.h>
#include <IO/Operators.h>

#include <Formats/verbosePrintString.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeNothing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}


CSVRowInputFormat::CSVRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_,
                                     bool with_names_, const FormatSettings & format_settings_)
    : RowInputFormatWithDiagnosticInfo(header_, in_, params_)
    , with_names(with_names_)
    , format_settings(format_settings_)
{

    const String bad_delimiters = " \t\"'.UL";
    if (bad_delimiters.find(format_settings.csv.delimiter) != String::npos)
        throw Exception(String("CSV format may not work correctly with delimiter '") + format_settings.csv.delimiter +
                        "'. Try use CustomSeparated format instead.", ErrorCodes::BAD_ARGUMENTS);

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
}


/// Map an input file column to a table column, based on its name.
void CSVRowInputFormat::addInputColumn(const String & column_name)
{
    const auto column_it = column_indexes_by_names.find(column_name);
    if (column_it == column_indexes_by_names.end())
    {
        if (format_settings.skip_unknown_fields)
        {
            column_mapping->column_indexes_for_input_fields.push_back(std::nullopt);
            return;
        }

        throw Exception(
                "Unknown field found in CSV header: '" + column_name + "' " +
                "at position " + std::to_string(column_mapping->column_indexes_for_input_fields.size()) +
                "\nSet the 'input_format_skip_unknown_fields' parameter explicitly to ignore and proceed",
                ErrorCodes::INCORRECT_DATA
        );
    }

    const auto column_index = column_it->second;

    if (column_mapping->read_columns[column_index])
        throw Exception("Duplicate field found while parsing CSV header: " + column_name, ErrorCodes::INCORRECT_DATA);

    column_mapping->read_columns[column_index] = true;
    column_mapping->column_indexes_for_input_fields.emplace_back(column_index);
}

static void skipEndOfLine(ReadBuffer & in)
{
    /// \n (Unix) or \r\n (DOS/Windows) or \n\r (Mac OS Classic)

    if (*in.position() == '\n')
    {
        ++in.position();
        if (!in.eof() && *in.position() == '\r')
            ++in.position();
    }
    else if (*in.position() == '\r')
    {
        ++in.position();
        if (!in.eof() && *in.position() == '\n')
            ++in.position();
        else
            throw Exception("Cannot parse CSV format: found \\r (CR) not followed by \\n (LF)."
                " Line must end by \\n (LF) or \\r\\n (CR LF) or \\n\\r.", ErrorCodes::INCORRECT_DATA);
    }
    else if (!in.eof())
        throw Exception("Expected end of line", ErrorCodes::INCORRECT_DATA);
}


static void skipDelimiter(ReadBuffer & in, const char delimiter, bool is_last_column)
{
    if (is_last_column)
    {
        if (in.eof())
            return;

        /// we support the extra delimiter at the end of the line
        if (*in.position() == delimiter)
        {
            ++in.position();
            if (in.eof())
                return;
        }

        skipEndOfLine(in);
    }
    else
        assertChar(delimiter, in);
}


/// Skip `whitespace` symbols allowed in CSV.
static inline void skipWhitespacesAndTabs(ReadBuffer & in)
{
    while (!in.eof()
            && (*in.position() == ' '
                || *in.position() == '\t'))
        ++in.position();
}


static void skipRow(ReadBuffer & in, const FormatSettings::CSV & settings, size_t num_columns)
{
    String tmp;
    for (size_t i = 0; i < num_columns; ++i)
    {
        skipWhitespacesAndTabs(in);
        readCSVString(tmp, in, settings);
        skipWhitespacesAndTabs(in);

        skipDelimiter(in, settings.delimiter, i + 1 == num_columns);
    }
}

void CSVRowInputFormat::setupAllColumnsByTableSchema()
{
    const auto & header = getPort().getHeader();
    column_mapping->read_columns.assign(header.columns(), true);
    column_mapping->column_indexes_for_input_fields.resize(header.columns());

    for (size_t i = 0; i < column_mapping->column_indexes_for_input_fields.size(); ++i)
        column_mapping->column_indexes_for_input_fields[i] = i;
}


void CSVRowInputFormat::readPrefix()
{
    /// In this format, we assume, that if first string field contain BOM as value, it will be written in quotes,
    ///  so BOM at beginning of stream cannot be confused with BOM in first string value, and it is safe to skip it.
    skipBOMIfExists(in);

    size_t num_columns = data_types.size();
    const auto & header = getPort().getHeader();

    /// This is a bit of abstraction leakage, but we have almost the same code in other places.
    /// Thus, we check if this InputFormat is working with the "real" beginning of the data in case of parallel parsing.
    if (with_names && getCurrentUnitNumber() == 0)
    {
        /// This CSV file has a header row with column names. Depending on the
        /// settings, use it or skip it.
        if (format_settings.with_names_use_header)
        {
            /// Look at the file header to see which columns we have there.
            /// The missing columns are filled with defaults.
            column_mapping->read_columns.assign(header.columns(), false);
            do
            {
                String column_name;
                skipWhitespacesAndTabs(in);
                readCSVString(column_name, in, format_settings.csv);
                skipWhitespacesAndTabs(in);

                addInputColumn(column_name);
            }
            while (checkChar(format_settings.csv.delimiter, in));

            skipDelimiter(in, format_settings.csv.delimiter, true);

            for (auto read_column : column_mapping->read_columns)
            {
                if (!read_column)
                {
                    column_mapping->have_always_default_columns = true;
                    break;
                }
            }

            return;
        }
        else
        {
            skipRow(in, format_settings.csv, num_columns);
            setupAllColumnsByTableSchema();
        }
    }
    else if (!column_mapping->is_set)
        setupAllColumnsByTableSchema();
}


bool CSVRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in.eof())
        return false;

    updateDiagnosticInfo();

    /// Track whether we have to fill any columns in this row with default
    /// values. If not, we return an empty column mask to the caller, so that
    /// it doesn't have to check it.
    bool have_default_columns = column_mapping->have_always_default_columns;

    ext.read_columns.assign(column_mapping->read_columns.size(), true);
    const auto delimiter = format_settings.csv.delimiter;
    for (size_t file_column = 0; file_column < column_mapping->column_indexes_for_input_fields.size(); ++file_column)
    {
        const auto & table_column = column_mapping->column_indexes_for_input_fields[file_column];
        const bool is_last_file_column = file_column + 1 == column_mapping->column_indexes_for_input_fields.size();

        if (table_column)
        {
            skipWhitespacesAndTabs(in);
            ext.read_columns[*table_column] = readField(*columns[*table_column], data_types[*table_column],
                serializations[*table_column], is_last_file_column);

            if (!ext.read_columns[*table_column])
                have_default_columns = true;
            skipWhitespacesAndTabs(in);
        }
        else
        {
            /// We never read this column from the file, just skip it.
            String tmp;
            readCSVString(tmp, in, format_settings.csv);
        }

        skipDelimiter(in, delimiter, is_last_file_column);
    }

    if (have_default_columns)
    {
        for (size_t i = 0; i < column_mapping->read_columns.size(); i++)
        {
            if (!column_mapping->read_columns[i])
            {
                /// The column value for this row is going to be overwritten
                /// with default by the caller, but the general assumption is
                /// that the column size increases for each row, so we have
                /// to insert something. Since we do not care about the exact
                /// value, we do not have to use the default value specified by
                /// the data type, and can just use IColumn::insertDefault().
                columns[i]->insertDefault();
                ext.read_columns[i] = false;
            }
        }
    }

    return true;
}

bool CSVRowInputFormat::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    const char delimiter = format_settings.csv.delimiter;

    for (size_t file_column = 0; file_column < column_mapping->column_indexes_for_input_fields.size(); ++file_column)
    {
        if (file_column == 0 && in.eof())
        {
            out << "<End of stream>\n";
            return false;
        }

        skipWhitespacesAndTabs(in);
        if (column_mapping->column_indexes_for_input_fields[file_column].has_value())
        {
            const auto & header = getPort().getHeader();
            size_t col_idx = column_mapping->column_indexes_for_input_fields[file_column].value();
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
        skipWhitespacesAndTabs(in);

        /// Delimiters
        if (file_column + 1 == column_mapping->column_indexes_for_input_fields.size())
        {
            if (in.eof())
                return false;

            /// we support the extra delimiter at the end of the line
            if (*in.position() == delimiter)
            {
                ++in.position();
                if (in.eof())
                    break;
            }

            if (!in.eof() && *in.position() != '\n' && *in.position() != '\r')
            {
                out << "ERROR: There is no line feed. ";
                verbosePrintString(in.position(), in.position() + 1, out);
                out << " found instead.\n"
                       " It's like your file has more columns than expected.\n"
                       "And if your file have right number of columns, maybe it have unquoted string value with comma.\n";

                return false;
            }

            skipEndOfLine(in);
        }
        else
        {
            try
            {
                assertChar(delimiter, in);
            }
            catch (const DB::Exception &)
            {
                if (*in.position() == '\n' || *in.position() == '\r')
                {
                    out << "ERROR: Line feed found where delimiter (" << delimiter << ") is expected."
                           " It's like your file has less columns than expected.\n"
                           "And if your file have right number of columns, maybe it have unescaped quotes in values.\n";
                }
                else
                {
                    out << "ERROR: There is no delimiter (" << delimiter << "). ";
                    verbosePrintString(in.position(), in.position() + 1, out);
                    out << " found instead.\n";
                }
                return false;
            }
        }
    }

    return true;
}


void CSVRowInputFormat::syncAfterError()
{
    skipToNextLineOrEOF(in);
}

void CSVRowInputFormat::tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column)
{
    const auto & index = column_mapping->column_indexes_for_input_fields[file_column];
    if (index)
    {
        const bool is_last_file_column = file_column + 1 == column_mapping->column_indexes_for_input_fields.size();
        readField(column, type, serializations[*index], is_last_file_column);
    }
    else
    {
        String tmp;
        readCSVString(tmp, in, format_settings.csv);
    }
}

bool CSVRowInputFormat::readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column)
{
    const bool at_delimiter = !in.eof() && *in.position() == format_settings.csv.delimiter;
    const bool at_last_column_line_end = is_last_file_column
                                         && (in.eof() || *in.position() == '\n' || *in.position() == '\r');

    /// Note: Tuples are serialized in CSV as separate columns, but with empty_as_default or null_as_default
    /// only one empty or NULL column will be expected
    if (format_settings.csv.empty_as_default
        && (at_delimiter || at_last_column_line_end))
    {
        /// Treat empty unquoted column value as default value, if
        /// specified in the settings. Tuple columns might seem
        /// problematic, because they are never quoted but still contain
        /// commas, which might be also used as delimiters. However,
        /// they do not contain empty unquoted fields, so this check
        /// works for tuples as well.
        column.insertDefault();
        return false;
    }
    else if (format_settings.null_as_default && !type->isNullable())
    {
        /// If value is null but type is not nullable then use default value instead.
        return SerializationNullable::deserializeTextCSVImpl(column, in, format_settings, serialization);
    }
    else
    {
        /// Read the column normally.
        serialization->deserializeTextCSV(column, in, format_settings);
        return true;
    }
}

void CSVRowInputFormat::resetParser()
{
    RowInputFormatWithDiagnosticInfo::resetParser();
    column_mapping->column_indexes_for_input_fields.clear();
    column_mapping->read_columns.clear();
    column_mapping->have_always_default_columns = false;
}


void registerInputFormatProcessorCSV(FormatFactory & factory)
{
    for (bool with_names : {false, true})
    {
        factory.registerInputFormatProcessor(with_names ? "CSVWithNames" : "CSV", [=](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<CSVRowInputFormat>(sample, buf, params, with_names, settings);
        });
    }
}

static std::pair<bool, size_t> fileSegmentationEngineCSVImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
{
    char * pos = in.position();
    bool quotes = false;
    bool need_more_data = true;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        if (quotes)
        {
            pos = find_first_symbols<'"'>(pos, in.buffer().end());
            if (pos > in.buffer().end())
                throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
            else if (pos == in.buffer().end())
                continue;
            else if (*pos == '"')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '"')
                    ++pos;
                else
                    quotes = false;
            }
        }
        else
        {
            pos = find_first_symbols<'"', '\r', '\n'>(pos, in.buffer().end());
            if (pos > in.buffer().end())
                throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
            else if (pos == in.buffer().end())
                continue;
            else if (*pos == '"')
            {
                quotes = true;
                ++pos;
            }
            else if (*pos == '\n')
            {
                ++number_of_rows;
                if (memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size)
                    need_more_data = false;
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '\r')
                    ++pos;
            }
            else if (*pos == '\r')
            {
                if (memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size)
                    need_more_data = false;
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '\n')
                {
                    ++pos;
                    ++number_of_rows;
                }
            }
        }
    }

    saveUpToPosition(in, memory, pos);
    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineCSV(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("CSV", &fileSegmentationEngineCSVImpl);
    factory.registerFileSegmentationEngine("CSVWithNames", &fileSegmentationEngineCSVImpl);
}

}
