#include <IO/ReadHelpers.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/Operators.h>

#include <Formats/verbosePrintString.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
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

CSVRowInputFormat::CSVRowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(header_, in_, params_, with_names_, with_types_, format_settings_)
{
    const String bad_delimiters = " \t\"'.UL";
    if (bad_delimiters.find(format_settings.csv.delimiter) != String::npos)
        throw Exception(
            String("CSV format may not work correctly with delimiter '") + format_settings.csv.delimiter
                + "'. Try use CustomSeparated format instead.",
            ErrorCodes::BAD_ARGUMENTS);
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

/// Skip `whitespace` symbols allowed in CSV.
static inline void skipWhitespacesAndTabs(ReadBuffer & in)
{
    while (!in.eof()
            && (*in.position() == ' '
                || *in.position() == '\t'))
        ++in.position();
}

void CSVRowInputFormat::skipFieldDelimiter()
{
    skipWhitespacesAndTabs(*in);
    assertChar(format_settings.csv.delimiter, *in);
}

String CSVRowInputFormat::readFieldIntoString()
{
    skipWhitespacesAndTabs(*in);
    String field;
    readCSVString(field, *in, format_settings.csv);
    return field;
}

void CSVRowInputFormat::skipField()
{
    readFieldIntoString();
}

void CSVRowInputFormat::skipRowEndDelimiter()
{
    skipWhitespacesAndTabs(*in);

    if (in->eof())
        return;

    /// we support the extra delimiter at the end of the line
    if (*in->position() == format_settings.csv.delimiter)
        ++in->position();

    skipWhitespacesAndTabs(*in);
    if (in->eof())
        return;

    skipEndOfLine(*in);
}

void CSVRowInputFormat::skipHeaderRow()
{
    do
    {
        skipField();
        skipWhitespacesAndTabs(*in);
    }
    while (checkChar(format_settings.csv.delimiter, *in));

    skipRowEndDelimiter();
}

std::vector<String> CSVRowInputFormat::readHeaderRow()
{
    std::vector<String> fields;
    do
    {
        fields.push_back(readFieldIntoString());
        skipWhitespacesAndTabs(*in);
    }
    while (checkChar(format_settings.csv.delimiter, *in));

    skipRowEndDelimiter();
    return fields;
}

bool CSVRowInputFormat::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    const char delimiter = format_settings.csv.delimiter;

    try
    {
        skipWhitespacesAndTabs(*in);
        assertChar(delimiter, *in);
    }
    catch (const DB::Exception &)
    {
        if (*in->position() == '\n' || *in->position() == '\r')
        {
            out << "ERROR: Line feed found where delimiter (" << delimiter << ") is expected."
                   " It's like your file has less columns than expected.\n"
                   "And if your file has the right number of columns, maybe it has unescaped quotes in values.\n";
        }
        else
        {
            out << "ERROR: There is no delimiter (" << delimiter << "). ";
            verbosePrintString(in->position(), in->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

bool CSVRowInputFormat::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    skipWhitespacesAndTabs(*in);

    if (in->eof())
        return true;

    /// we support the extra delimiter at the end of the line
    if (*in->position() == format_settings.csv.delimiter)
    {
        ++in->position();
        skipWhitespacesAndTabs(*in);
        if (in->eof())
            return true;
    }

    if (!in->eof() && *in->position() != '\n' && *in->position() != '\r')
    {
        out << "ERROR: There is no line feed. ";
        verbosePrintString(in->position(), in->position() + 1, out);
        out << " found instead.\n"
               " It's like your file has more columns than expected.\n"
               "And if your file has the right number of columns, maybe it has an unquoted string value with a comma.\n";

        return false;
    }

    skipEndOfLine(*in);
    return true;
}

void CSVRowInputFormat::syncAfterError()
{
    skipToNextLineOrEOF(*in);
}

bool CSVRowInputFormat::readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & /*column_name*/)
{
    skipWhitespacesAndTabs(*in);

    const bool at_delimiter = !in->eof() && *in->position() == format_settings.csv.delimiter;
    const bool at_last_column_line_end = is_last_file_column
                                         && (in->eof() || *in->position() == '\n' || *in->position() == '\r');

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
    else if (format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable())
    {
        /// If value is null but type is not nullable then use default value instead.
        return SerializationNullable::deserializeTextCSVImpl(column, *in, format_settings, serialization);
    }
    else
    {
        /// Read the column normally.
        serialization->deserializeTextCSV(column, *in, format_settings);
        return true;
    }
}

void registerInputFormatCSV(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerInputFormat(format_name, [with_names, with_types](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<CSVRowInputFormat>(sample, buf, std::move(params), with_names, with_types, settings);
        });
    };

    registerWithNamesAndTypes("CSV", register_func);
}

static std::pair<bool, size_t> fileSegmentationEngineCSVImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows)
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
                if (memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size && number_of_rows >= min_rows)
                    need_more_data = false;
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '\r')
                    ++pos;
            }
            else if (*pos == '\r')
            {
                if (memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size && number_of_rows >= min_rows)
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
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        size_t min_rows = 1 + int(with_names) + int(with_types);
        factory.registerFileSegmentationEngine(format_name, [min_rows](ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
        {
            return fileSegmentationEngineCSVImpl(in, memory, min_chunk_size, min_rows);
        });
    };

    registerWithNamesAndTypes("CSV", register_func);
}

}
