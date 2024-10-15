#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
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
#include <Common/logger_useful.h>


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
    void checkBadDelimiter(char delimiter, bool allow_whitespace_or_tab_as_delimiter)
    {
        if ((delimiter == ' ' || delimiter == '\t') && allow_whitespace_or_tab_as_delimiter)
        {
            return;
        }
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
    std::unique_ptr<CSVFormatReader> format_reader_)
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
    checkBadDelimiter(format_settings_.csv.delimiter, format_settings_.csv.allow_whitespace_or_tab_as_delimiter);
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
    checkBadDelimiter(format_settings_.csv.delimiter, format_settings_.csv.allow_whitespace_or_tab_as_delimiter);
}

void CSVRowInputFormat::syncAfterError()
{
    skipToNextLineOrEOF(*buf);
}

void CSVRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    RowInputFormatWithNamesAndTypes::setReadBuffer(*buf);
}

void CSVRowInputFormat::resetReadBuffer()
{
    buf.reset();
    RowInputFormatWithNamesAndTypes::resetReadBuffer();
}

void CSVFormatReader::skipRow()
{
    bool quotes = false;
    ReadBuffer & istr = *buf;

    while (!istr.eof())
    {
        if (quotes)
        {
            auto * pos = find_first_symbols<'"'>(istr.position(), istr.buffer().end());
            istr.position() = pos;

            if (pos > istr.buffer().end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
            if (pos == istr.buffer().end())
                continue;
            if (*pos == '"')
            {
                ++istr.position();
                if (!istr.eof() && *istr.position() == '"')
                    ++istr.position();
                else
                    quotes = false;
            }
        }
        else
        {
            auto * pos = find_first_symbols<'"', '\r', '\n'>(istr.position(), istr.buffer().end());
            istr.position() = pos;

            if (pos > istr.buffer().end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
            if (pos == istr.buffer().end())
                continue;

            if (*pos == '"')
            {
                quotes = true;
                ++istr.position();
                continue;
            }

            if (*pos == '\n')
            {
                ++istr.position();
                if (!istr.eof() && *istr.position() == '\r')
                    ++istr.position();
                return;
            }
            if (*pos == '\r')
            {
                ++istr.position();
                if (format_settings.csv.allow_cr_end_of_line)
                    return;
                if (!istr.eof() && *pos == '\n')
                {
                    ++pos;
                    return;
                }
            }
        }
    }
}

static void skipEndOfLine(ReadBuffer & in, bool allow_cr_end_of_line)
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
        else if (!allow_cr_end_of_line)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Cannot parse CSV format: found \\r (CR) not followed by \\n (LF)."
                " Line must end by \\n (LF) or \\r\\n (CR LF) or \\n\\r.");
    }
    else if (!in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected end of line");
}

/// Skip `whitespace` symbols allowed in CSV.
static inline void skipWhitespacesAndTabs(ReadBuffer & in, const bool & allow_whitespace_or_tab_as_delimiter)
{
    if (allow_whitespace_or_tab_as_delimiter)
    {
        return;
    }
    while (!in.eof() && (*in.position() == ' ' || *in.position() == '\t'))
        ++in.position();
}

CSVFormatReader::CSVFormatReader(PeekableReadBuffer & buf_, const FormatSettings & format_settings_) : FormatWithNamesAndTypesReader(buf_, format_settings_), buf(&buf_)
{
}

void CSVFormatReader::skipFieldDelimiter()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    assertChar(format_settings.csv.delimiter, *buf);
}

template <bool read_string>
String CSVFormatReader::readCSVFieldIntoString()
{
    if (format_settings.csv.trim_whitespaces) [[likely]]
        skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    String field;
    if constexpr (read_string)
        readCSVString(field, *buf, format_settings.csv);
    else
        readCSVField(field, *buf, format_settings.csv);
    return field;
}

void CSVFormatReader::skipField()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    NullOutput out;
    readCSVStringInto(out, *buf, format_settings.csv);
}

void CSVFormatReader::skipRowEndDelimiter()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    if (buf->eof())
        return;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv.delimiter)
        ++buf->position();

    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    if (buf->eof())
        return;

    skipEndOfLine(*buf, format_settings.csv.allow_cr_end_of_line);
}

void CSVFormatReader::skipHeaderRow()
{
    do
    {
        skipField();
        skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
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
        skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    } while (checkChar(format_settings.csv.delimiter, *buf));

    skipRowEndDelimiter();
    return fields;
}

bool CSVFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    const char delimiter = format_settings.csv.delimiter;

    try
    {
        skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
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
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    if (buf->eof())
        return true;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv.delimiter)
    {
        ++buf->position();
        skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
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

    skipEndOfLine(*buf, format_settings.csv.allow_cr_end_of_line);
    return true;
}

bool CSVFormatReader::allowVariableNumberOfColumns() const
{
    return format_settings.csv.allow_variable_number_of_columns;
}

bool CSVFormatReader::readField(
    IColumn & column,
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    bool is_last_file_column,
    const String & /*column_name*/)
{
    if (format_settings.csv.trim_whitespaces || !isStringOrFixedString(removeNullable(type))) [[likely]]
        skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    const bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv.delimiter;
    const bool at_last_column_line_end = is_last_file_column && (buf->eof() || *buf->position() == '\n' || *buf->position() == '\r');

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
        return false;
    }

    if (format_settings.csv.use_default_on_bad_values)
        return readFieldOrDefault(column, type, serialization);
    return readFieldImpl(*buf, column, type, serialization);
}

bool CSVFormatReader::readFieldImpl(ReadBuffer & istr, DB::IColumn & column, const DB::DataTypePtr & type, const DB::SerializationPtr & serialization)
{
    if (format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type))
    {
        /// If value is null but type is not nullable then use default value instead.
        return SerializationNullable::deserializeNullAsDefaultOrNestedTextCSV(column, istr, format_settings, serialization);
    }

    /// Read the column normally.
    serialization->deserializeTextCSV(column, istr, format_settings);
    return true;
}

bool CSVFormatReader::readFieldOrDefault(DB::IColumn & column, const DB::DataTypePtr & type, const DB::SerializationPtr & serialization)
{
    String field;
    readCSVField(field, *buf, format_settings.csv);
    ReadBufferFromString tmp_buf(field);
    bool is_bad_value = false;
    bool res = false;

    size_t col_size = column.size();
    try
    {
        res = readFieldImpl(tmp_buf, column, type, serialization);
        /// Check if we parsed the whole field successfully.
        if (!field.empty() && !tmp_buf.eof())
            is_bad_value = true;
    }
    catch (const Exception &)
    {
        is_bad_value = true;
    }

    if (!is_bad_value)
        return res;

    if (column.size() == col_size + 1)
        column.popBack(1);
    column.insertDefault();
    return false;
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

bool CSVFormatReader::checkForEndOfRow()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    return buf->eof() || *buf->position() == '\n' || *buf->position() == '\r';
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

std::optional<std::pair<std::vector<String>, DataTypes>> CSVSchemaReader::readRowAndGetFieldsAndDataTypes()
{
    if (buf.eof())
        return {};

    auto fields = reader.readRow();
    auto data_types = tryInferDataTypesByEscapingRule(fields, format_settings, FormatSettings::EscapingRule::CSV);
    return std::make_pair(std::move(fields), std::move(data_types));
}

std::optional<DataTypes> CSVSchemaReader::readRowAndGetDataTypesImpl()
{
    auto fields_with_types = readRowAndGetFieldsAndDataTypes();
    if (!fields_with_types)
        return {};
    return std::move(fields_with_types->second);
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

std::pair<bool, size_t> fileSegmentationEngineCSVImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows, const FormatSettings & settings)
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
            if (pos == in.buffer().end())
                continue;
            if (*pos == '"')
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
            if (pos == in.buffer().end())
                continue;

            if (*pos == '"')
            {
                quotes = true;
                ++pos;
                continue;
            }

            if (*pos == '\n')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '\r')
                    ++pos;
            }
            else if (*pos == '\r')
            {
                ++pos;
                if (settings.csv.allow_cr_end_of_line)
                    continue;
                if (loadAtPosition(in, memory, pos) && *pos == '\n')
                    ++pos;
                else
                    continue;
            }

            ++number_of_rows;
            if ((number_of_rows >= min_rows)
                && ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows)))
                need_more_data = false;

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
        factory.registerFileSegmentationEngineCreator(format_name, [](const FormatSettings & settings) -> FormatFactory::FileSegmentationEngine
        {
            return [settings] (ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
            {
                return fileSegmentationEngineCSVImpl(in, memory, min_bytes, min_rows, max_rows, settings);
            };
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
                    result += fmt::format(", column_names_for_schema_inference={}, try_detect_header={}, skip_first_lines={}", settings.column_names_for_schema_inference, settings.csv.try_detect_header, settings.csv.skip_first_lines);
                return result;
            });
        }
    };

    registerWithNamesAndTypes("CSV", register_func);
}

}
