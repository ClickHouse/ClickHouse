#include <IO/ReadHelpers.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>

#include <Formats/verbosePrintString.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>


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
    bool hasTupleType(const DataTypes & types)
    {
        for (const auto & type : types)
            if (isTuple(type))
                return true;
        return false;
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
        header_, in_, params_, with_names_, with_types_, format_settings_, std::make_unique<CSVFormatReader>(in_, format_settings_))
{
}

CSVRowInputFormat::CSVRowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_,
    std::unique_ptr<FormatWithNamesAndTypesReader> format_reader_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        in_,
        params_,
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::move(format_reader_),
        /// We cannot detect header properly when we have Tuple in CSV, because tuple name is a single column, but tuple elements are parsed as separate CSV columns.
        format_settings_.csv.try_detect_header && !hasTupleType(header_.getDataTypes()))
{
    const String bad_delimiters = " \t\"'.UL";
    if (bad_delimiters.find(format_settings.csv.delimiter) != String::npos)
        throw Exception(
            String("CSV format may not work correctly with delimiter '") + format_settings.csv.delimiter
                + "'. Try use CustomSeparated format instead.",
            ErrorCodes::BAD_ARGUMENTS);
}

void CSVRowInputFormat::syncAfterErrorImpl()
{
    skipToNextLineOrEOF(*in);
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
            throw Exception(
                "Cannot parse CSV format: found \\r (CR) not followed by \\n (LF)."
                " Line must end by \\n (LF) or \\r\\n (CR LF) or \\n\\r.",
                ErrorCodes::INCORRECT_DATA);
    }
    else if (!in.eof())
        throw Exception("Expected end of line", ErrorCodes::INCORRECT_DATA);
}

/// Skip `whitespace` symbols allowed in CSV.
static inline void skipWhitespacesAndTabs(ReadBuffer & in)
{
    while (!in.eof() && (*in.position() == ' ' || *in.position() == '\t'))
        ++in.position();
}

CSVFormatReader::CSVFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_) : FormatWithNamesAndTypesReader(in_, format_settings_)
{
}

void CSVFormatReader::skipFieldDelimiter()
{
    skipWhitespacesAndTabs(*in);
    assertChar(format_settings.csv.delimiter, *in);
}

template <bool read_string>
String CSVFormatReader::readCSVFieldIntoString()
{
    skipWhitespacesAndTabs(*in);
    String field;
    if constexpr (read_string)
        readCSVString(field, *in, format_settings.csv);
    else
        readCSVField(field, *in, format_settings.csv);
    return field;
}

void CSVFormatReader::skipField()
{
    skipWhitespacesAndTabs(*in);
    NullOutput out;
    readCSVStringInto(out, *in, format_settings.csv);
}

void CSVFormatReader::skipRowEndDelimiter()
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

void CSVFormatReader::skipHeaderRow()
{
    do
    {
        skipField();
        skipWhitespacesAndTabs(*in);
    } while (checkChar(format_settings.csv.delimiter, *in));

    skipRowEndDelimiter();
}

template <bool is_header>
std::vector<String> CSVFormatReader::readRowImpl()
{
    std::vector<String> fields;
    do
    {
        fields.push_back(readCSVFieldIntoString<is_header>());
        skipWhitespacesAndTabs(*in);
    } while (checkChar(format_settings.csv.delimiter, *in));

    skipRowEndDelimiter();
    return fields;
}

bool CSVFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
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
            out << "ERROR: Line feed found where delimiter (" << delimiter
                << ") is expected."
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

bool CSVFormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
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

bool CSVFormatReader::readField(
    IColumn & column,
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    bool is_last_file_column,
    const String & /*column_name*/)
{
    skipWhitespacesAndTabs(*in);

    const bool at_delimiter = !in->eof() && *in->position() == format_settings.csv.delimiter;
    const bool at_last_column_line_end = is_last_file_column && (in->eof() || *in->position() == '\n' || *in->position() == '\r');

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

    return readFieldImpl(*in, column, type, serialization);
}

/// Similar to readField above, but read value from string.
bool CSVFormatReader::readField(const String & field, IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, const String &)
{
    if (format_settings.csv.empty_as_default && field.empty())
    {
        column.insertDefault();
        return false;
    }

    ReadBufferFromString buf(field);
    auto res = readFieldImpl(buf, column, type, serialization);
    if (!buf.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse value of type {} here: \"{}\"", type->getName(), field);
    return res;
}

bool CSVFormatReader::readFieldImpl(ReadBuffer & buf, IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization)
{
    if (format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable())
    {
        /// If value is null but type is not nullable then use default value instead.
        return SerializationNullable::deserializeTextCSVImpl(column, buf, format_settings, serialization);
    }

    /// Read the column normally.
    serialization->deserializeTextCSV(column, buf, format_settings);
    return true;
}

void CSVFormatReader::skipPrefixBeforeHeader()
{
    for (size_t i = 0; i != format_settings.csv.skip_first_lines; ++i)
        readRow();
}


CSVSchemaReader::CSVSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(
        in_,
        format_settings_,
        with_names_,
        with_types_,
        &reader,
        getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule::CSV),
        format_settings_.csv.try_detect_header)
    , reader(in_, format_settings_)
{
}

std::pair<std::vector<String>, DataTypes> CSVSchemaReader::readRowAndGetFieldsAndDataTypes()
{
    if (in.eof())
        return {};

    auto fields = reader.readRow();
    auto data_types = tryInferDataTypesByEscapingRule(fields, format_settings, FormatSettings::EscapingRule::CSV);
    return {fields, data_types};}

DataTypes CSVSchemaReader::readRowAndGetDataTypesImpl()
{
    return readRowAndGetFieldsAndDataTypes().second;
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
        size_t min_rows = 3; /// Make it 3 for header auto detection (first 3 rows must be always in the same segment).
        factory.registerFileSegmentationEngine(format_name, [min_rows](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
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
