#include <IO/ReadHelpers.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/Operators.h>

#include <Formats/verbosePrintString.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void checkBadDelimiter(char delimiter)
    {
        constexpr std::string_view bad_delimiters = " \t\"'.UL";
        if (bad_delimiters.find(delimiter) != std::string_view::npos)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "CSV format may not work correctly with delimiter '{}'. Try use CustomSeparated format instead",
                delimiter);
    }
}

CSVRowInputFormat::CSVRowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_)
    : CSVRowInputFormat(
        header_, std::make_shared<PeekableReadBuffer>(in_), params_, with_names_, with_types_, format_settings_)
{
}

CSVRowInputFormat::CSVRowInputFormat(
    const Block & header_,
    std::shared_ptr<PeekableReadBuffer> in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_,
    std::unique_ptr<FormatWithNamesAndTypesReader> format_reader_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        *in_,
        params_,
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::move(format_reader_),
        format_settings_.csv.try_detect_header),
    buf(std::move(in_))
{
    checkBadDelimiter(format_settings_.csv.delimiter);
}

CSVRowInputFormat::CSVRowInputFormat(
    const Block & header_,
    std::shared_ptr<PeekableReadBuffer> in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        *in_,
        params_,
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<CSVFormatReader>(*in_, format_settings_),
        format_settings_.csv.try_detect_header),
    buf(std::move(in_))
{
    checkBadDelimiter(format_settings_.csv.delimiter);
}

void CSVRowInputFormat::syncAfterError()
{
    skipToNextLineOrEOF(*buf);
}

void CSVRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf->setSubBuffer(in_);
}

void CSVRowInputFormat::resetParser()
{
    RowInputFormatWithNamesAndTypes::resetParser();
    buf->reset();
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
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Cannot parse CSV format: found \\r (CR) not followed by \\n (LF)."
                " Line must end by \\n (LF) or \\r\\n (CR LF) or \\n\\r.");
    }
    else if (!in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected end of line");
}

/// Skip `whitespace` symbols allowed in CSV.
static inline void skipWhitespacesAndTabs(ReadBuffer & in)
{
    while (!in.eof() && (*in.position() == ' ' || *in.position() == '\t'))
        ++in.position();
}

CSVFormatReader::CSVFormatReader(PeekableReadBuffer & buf_, const FormatSettings & format_settings_) : FormatWithNamesAndTypesReader(buf_, format_settings_), buf(&buf_)
{
}

void CSVFormatReader::skipFieldDelimiter()
{
    skipWhitespacesAndTabs(*buf);

    bool res = checkChar(format_settings.csv.delimiter, *buf);
    if (!res)
    {
        if (!format_settings.csv.missing_as_default)
        {
            char err[2] = {format_settings.csv.delimiter, '\0'};
            throwAtAssertionFailed(err, *buf);
        }
        else
        {
            current_row_has_missing_fields = true;
        }
    }
}

template <bool read_string>
String CSVFormatReader::readCSVFieldIntoString()
{
    if (format_settings.csv.trim_whitespaces) [[likely]]
        skipWhitespacesAndTabs(*buf);

    String field;
    if constexpr (read_string)
        readCSVString(field, *buf, format_settings.csv);
    else
        readCSVField(field, *buf, format_settings.csv);
    return field;
}

void CSVFormatReader::skipField()
{
    skipWhitespacesAndTabs(*buf);
    NullOutput out;
    readCSVStringInto(out, *buf, format_settings.csv);
}

void CSVFormatReader::skipRowEndDelimiter()
{
    skipWhitespacesAndTabs(*buf);

    if (buf->eof())
        return;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv.delimiter)
        ++buf->position();

    skipWhitespacesAndTabs(*buf);
    if (buf->eof())
        return;

    skipEndOfLine(*buf);
    current_row_has_missing_fields = false;
}

void CSVFormatReader::skipHeaderRow()
{
    do
    {
        skipField();
        skipWhitespacesAndTabs(*buf);
    } while (checkChar(format_settings.csv.delimiter, *buf));

    skipRowEndDelimiter();
}

template <bool is_header>
std::vector<String> CSVFormatReader::readRowImpl()
{
    std::vector<String> fields;
    do
    {
        fields.push_back(readCSVFieldIntoString<is_header>());
        skipWhitespacesAndTabs(*buf);
    } while (checkChar(format_settings.csv.delimiter, *buf));

    skipRowEndDelimiter();
    return fields;
}

bool CSVFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    const char delimiter = format_settings.csv.delimiter;

    try
    {
        skipWhitespacesAndTabs(*buf);
        assertChar(delimiter, *buf);
    }
    catch (const DB::Exception &)
    {
        if (*buf->position() == '\n' || *buf->position() == '\r')
        {
            out << "ERROR: Line feed found where delimiter (" << delimiter
                << ") is expected."
                   " It's like your file has less columns than expected.\n"
                   "And if your file has the right number of columns, maybe it has unescaped quotes in values.\n";
        }
        else
        {
            out << "ERROR: There is no delimiter (" << delimiter << "). ";
            verbosePrintString(buf->position(), buf->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

bool CSVFormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    skipWhitespacesAndTabs(*buf);

    if (buf->eof())
        return true;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv.delimiter)
    {
        ++buf->position();
        skipWhitespacesAndTabs(*buf);
        if (buf->eof())
            return true;
    }

    if (!buf->eof() && *buf->position() != '\n' && *buf->position() != '\r')
    {
        out << "ERROR: There is no line feed. ";
        verbosePrintString(buf->position(), buf->position() + 1, out);
        out << " found instead.\n"
               " It's like your file has more columns than expected.\n"
               "And if your file has the right number of columns, maybe it has an unquoted string value with a comma.\n";

        return false;
    }

    skipEndOfLine(*buf);
    return true;
}

bool CSVFormatReader::readField(
    IColumn & column,
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    bool is_last_file_column,
    const String & /*column_name*/)
{
    if (format_settings.csv.trim_whitespaces || !isStringOrFixedString(removeNullable(type))) [[likely]]
        skipWhitespacesAndTabs(*buf);

    const bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv.delimiter;
    const bool at_last_column_line_end = is_last_file_column && (buf->eof() || *buf->position() == '\n' || *buf->position() == '\r');

    bool res = false;

    /// Note: Tuples are serialized in CSV as separate columns, but with empty_as_default or null_as_default
    /// only one empty or NULL column will be expected
    if (format_settings.csv.empty_as_default && (at_delimiter || at_last_column_line_end))
    {
        /// Treat empty unquoted column value as default value, if
        /// specified in the settings. Tuple columns might seem
        /// problematic, because they are never quoted but still contain
        /// commas, which might be also used as delimiters. However,
        /// they do not contain empty unquoted fields, so this check
        /// works for tuples as well.
        column.insertDefault();
    }
    else if (current_row_has_missing_fields)
    {
        column.insertDefault();
    }
    else if (format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type))
    {
        /// If value is null but type is not nullable then use default value instead.
        res = SerializationNullable::deserializeTextCSVImpl(column, *buf, format_settings, serialization);
    }
    else
    {
        /// Read the column normally.
        serialization->deserializeTextCSV(column, *buf, format_settings);
        res = true;
    }

    if (is_last_file_column && format_settings.csv.ignore_extra_columns)
    {
        // Skip all fields to next line.
        while (checkChar(format_settings.csv.delimiter, *buf))
        {
            skipField();
            skipWhitespacesAndTabs(*buf);
        }
    }
    return res;
}

void CSVFormatReader::skipPrefixBeforeHeader()
{
    for (size_t i = 0; i != format_settings.csv.skip_first_lines; ++i)
        readRow();
}

void CSVFormatReader::setReadBuffer(ReadBuffer & in_)
{
    buf = assert_cast<PeekableReadBuffer *>(&in_);
    FormatWithNamesAndTypesReader::setReadBuffer(*buf);
}

bool CSVFormatReader::checkForSuffix()
{
    if (!format_settings.csv.skip_trailing_empty_lines)
        return buf->eof();

    PeekableReadBufferCheckpoint checkpoint(*buf);
    while (checkChar('\n', *buf) || checkChar('\r', *buf));
    if (buf->eof())
        return true;

    buf->rollbackToCheckpoint();
    return false;
}

CSVSchemaReader::CSVSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(
        buf,
        format_settings_,
        with_names_,
        with_types_,
        &reader,
        getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule::CSV),
        format_settings_.csv.try_detect_header)
    , buf(in_)
    , reader(buf, format_settings_)
{
}

std::pair<std::vector<String>, DataTypes> CSVSchemaReader::readRowAndGetFieldsAndDataTypes()
{
    if (buf.eof())
        return {};

    auto fields = reader.readRow();
    auto data_types = tryInferDataTypesByEscapingRule(fields, format_settings, FormatSettings::EscapingRule::CSV);
    return {fields, data_types};
}

DataTypes CSVSchemaReader::readRowAndGetDataTypesImpl()
{
    return std::move(readRowAndGetFieldsAndDataTypes().second);
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

std::pair<bool, size_t> fileSegmentationEngineCSVImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows)
{
    char * pos = in.position();
    bool quotes = false;
    bool need_more_data = true;
    size_t number_of_rows = 0;

    if (max_rows && (max_rows < min_rows))
        max_rows = min_rows;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        if (quotes)
        {
            pos = find_first_symbols<'"'>(pos, in.buffer().end());
            if (pos > in.buffer().end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
            else if (pos == in.buffer().end())
                continue;

            if (*pos == '"')
            {
                quotes = true;
                ++pos;
                continue;
            }

            ++number_of_rows;
            if ((number_of_rows >= min_rows)
                && ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows)))
                need_more_data = false;

            if (*pos == '\n')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '\r')
                    ++pos;
            }
            else if (*pos == '\r')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '\n')
                    ++pos;
            }
        }
    }

    saveUpToPosition(in, memory, pos);
    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineCSV(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool, bool)
    {
        static constexpr size_t min_rows = 3; /// Make it 3 for header auto detection (first 3 rows must be always in the same segment).
        factory.registerFileSegmentationEngine(format_name, [](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
        {
            return fileSegmentationEngineCSVImpl(in, memory, min_bytes, min_rows, max_rows);
        });
    };

    registerWithNamesAndTypes("CSV", register_func);
    markFormatWithNamesAndTypesSupportsSamplingColumns("CSV", factory);
}

void registerCSVSchemaReader(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerSchemaReader(format_name, [with_names, with_types](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<CSVSchemaReader>(buf, with_names, with_types, settings);
        });
        if (!with_types)
        {
            factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [with_names](const FormatSettings & settings)
            {
                String result = getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::CSV);
                if (!with_names)
                    result += fmt::format(", column_names_for_schema_inference={}, try_detect_header={}", settings.column_names_for_schema_inference, settings.csv.try_detect_header);
                return result;
            });
        }
    };

    registerWithNamesAndTypes("CSV", register_func);
}

}
