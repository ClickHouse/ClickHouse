#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/Operators.h>

#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Formats/verbosePrintString.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>


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
        if (bad_delimiters.contains(delimiter))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "CSV format may not work correctly with delimiter '{}'. Try use CustomSeparated format instead",
                delimiter);
    }
}

CSVRowInputFormat::CSVRowInputFormat(
    SharedHeader header_,
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
    SharedHeader header_,
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
        format_settings_.csv.try_detect_header,
        format_settings_.csv.allow_variable_number_of_columns),
    buf(std::move(in_))
{
    checkBadDelimiter(format_settings_.csv.delimiter, format_settings_.csv.allow_whitespace_or_tab_as_delimiter);
}

CSVRowInputFormat::CSVRowInputFormat(
    SharedHeader header_,
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
        format_settings_.csv.try_detect_header,
        format_settings_.csv.allow_variable_number_of_columns),
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
        ///
        /// Exception: `Nullable(Tuple())` with zero elements serializes to
        /// an empty field in CSV, so an empty value is its only valid
        /// representation. Let it fall through to normal deserialization
        /// instead of inserting NULL as the default.
        ///
        /// Exception: with `input_format_csv_missing_nullable_as_empty_string`, a missing value of
        /// `Nullable(String)` (including its low-cardinality form `LowCardinality(Nullable(String))`)
        /// should be read as an empty string instead of NULL, so it must also fall through to
        /// normal deserialization.
        bool keep_empty_value = false;
        if (type->isNullable())
        {
            if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(type).get()))
                keep_empty_value = tuple_type->getElements().empty();
        }

        if (!keep_empty_value && format_settings.csv.missing_nullable_as_empty_string
            && isNullableOrLowCardinalityNullable(type) && isString(removeLowCardinalityAndNullable(type)))
            keep_empty_value = true;

        if (!keep_empty_value)
        {
            column.insertDefault();
            return false;
        }
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
    {
        if (buf->eof())
            break;
        readRow();
    }
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


void registerInputFormatCSV(FormatFactory & factory);
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
            return std::make_shared<CSVRowInputFormat>(std::make_shared<const Block>(sample), buf, std::move(params), with_names, with_types, settings);
        });
    };

    registerWithNamesAndTypes("CSV", register_func);

    factory.setDocumentation("CSV", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Comma Separated Values format ([RFC](https://tools.ietf.org/html/rfc4180)).
When formatting, rows are enclosed in double quotes. A double quote inside a string is output as two double quotes in a row. 
There are no other rules for escaping characters. 

- Date and date-time are enclosed in double quotes. 
- Numbers are output without quotes.
- Values are separated by a delimiter character, which is `,` by default. The delimiter character is defined in the setting [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter). 
- Rows are separated using the Unix line feed (LF). 
- Arrays are serialized in CSV as follows: 
  - first, the array is serialized to a string as in TabSeparated format
  - The resulting string is output to CSV in double quotes.
- Tuples in CSV format are serialized as separate columns (that is, their nesting in the tuple is lost).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
By default, the delimiter is `,` 
See the [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter) setting for more information.
:::

When parsing, all values can be parsed either with or without quotes. Both double and single quotes are supported.

Rows can also be arranged without quotes. In this case, they are parsed up to the delimiter character or line feed (CR or LF).
However, in violation of the RFC, when parsing rows without quotes, the leading and trailing spaces and tabs are ignored.
The line feed supports: Unix (LF), Windows (CR LF) and Mac OS Classic (CR LF) types.

`NULL` is formatted according to setting [format_csv_null_representation](/operations/settings/settings-formats.md/#format_csv_null_representation) (the default value is `\N`).

In the input data, `ENUM` values can be represented as names or as ids. 
First, we try to match the input value to the ENUM name. 
If we fail and the input value is a number, we try to match this number to the ENUM id.
If input data contains only ENUM ids, it's recommended to enable the setting [input_format_csv_enum_as_number](/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) to optimize `ENUM` parsing.

## Example usage {#example-usage}

## Format settings {#format-settings}

| Setting                                                                                                                                                            | Description                                                                                                        | Default | Notes                                                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                     | the character to be considered as a delimiter in CSV data.                                                         | `,`     |                                                                                                                                                                                              |
| [format_csv_allow_single_quotes](/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                 | allow strings in single quotes.                                                                                    | `true`  |                                                                                                                                                                                              |
| [format_csv_allow_double_quotes](/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                 | allow strings in double quotes.                                                                                    | `true`  |                                                                                                                                                                                              | 
| [format_csv_null_representation](/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                 | custom NULL representation in CSV format.                                                                          | `\N`    |                                                                                                                                                                                              |   
| [input_format_csv_empty_as_default](/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                           | treat empty fields in CSV input as default values.                                                                 | `true`  | For complex default expressions, [input_format_defaults_for_omitted_fields](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too. |
| [input_format_csv_missing_nullable_as_empty_string](/operations/settings/settings-formats.md/#input_format_csv_missing_nullable_as_empty_string)                           | read a missing value of `Nullable(String)` as an empty string instead of NULL, regardless of `input_format_csv_empty_as_default`. | `false` |                                                                                                                                                                                              |
| [input_format_csv_enum_as_number](/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                               | treat inserted enum values in CSV formats as enum indices.                                                         | `false` |                                                                                                                                                                                              |
| [input_format_csv_use_best_effort_in_schema_inference](/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)     | use some tweaks and heuristics to infer schema in CSV format. If disabled, all fields will be inferred as Strings. | `true`  |                                                                                                                                                                                              |
| [input_format_csv_arrays_as_nested_csv](/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                   | when reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string.      | `false` |                                                                                                                                                                                              |
| [output_format_csv_crlf_end_of_line](/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                         | if it is set to true, end of line in CSV output format will be `\r\n` instead of `\n`.                             | `false` |                                                                                                                                                                                              |
| [input_format_csv_skip_first_lines](/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                           | skip the specified number of lines at the beginning of data.                                                       | `0`     |                                                                                                                                                                                              |
| [input_format_csv_detect_header](/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                 | automatically detect header with names and types in CSV format.                                                    | `true`  |                                                                                                                                                                                              |
| [input_format_csv_skip_trailing_empty_lines](/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                         | skip trailing empty lines at the end of data.                                                                      | `false` |                                                                                                                                                                                              |
| [input_format_csv_trim_whitespaces](/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                           | trim spaces and tabs in non-quoted CSV strings.                                                                    | `true`  |                                                                                                                                                                                              |
| [input_format_csv_allow_whitespace_or_tab_as_delimiter](/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter)   | Allow to use whitespace or tab as field delimiter in CSV strings.                                                  | `false` |                                                                                                                                                                                              |
| [input_format_csv_allow_variable_number_of_columns](/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)           | allow variable number of columns in CSV format, ignore extra columns and use default values on missing columns.    | `false` |                                                                                                                                                                                              |
| [input_format_csv_use_default_on_bad_values](/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                         | Allow to set default value to column when CSV field deserialization failed on bad value.                           | `false` |                                                                                                                                                                                              |
| [input_format_csv_try_infer_numbers_from_strings](/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)               | Try to infer numbers from string fields while schema inference.                                                    | `false` |                                                                                                                                                                                              |
)DOCS_MD"});

    factory.setDocumentation("CSVWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Also prints the header row with column names, similar to [TabSeparatedWithNames](/interfaces/formats/TabSeparatedWithNames).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

:::tip
Starting from [version](https://github.com/ClickHouse/ClickHouse/releases) 23.1, ClickHouse will automatically detect headers in CSV files when using the `CSV` format, so it is not necessary to use `CSVWithNames` or `CSVWithNamesAndTypes`.
:::

Using the following CSV file, named as `football.csv`:

```csv
date,season,home_team,away_team,home_team_goals,away_team_goals
2022-04-30,2021,Sutton United,Bradford City,1,4
2022-04-30,2021,Swindon Town,Barrow,2,1
2022-04-30,2021,Tranmere Rovers,Oldham Athletic,2,0
2022-05-02,2021,Salford City,Mansfield Town,2,2
2022-05-02,2021,Port Vale,Newport County,1,2
2022-05-07,2021,Barrow,Northampton Town,1,3
2022-05-07,2021,Bradford City,Carlisle United,2,0
2022-05-07,2021,Bristol Rovers,Scunthorpe United,7,0
2022-05-07,2021,Exeter City,Port Vale,0,1
2022-05-07,2021,Harrogate Town A.F.C.,Sutton United,0,2
2022-05-07,2021,Hartlepool United,Colchester United,0,2
2022-05-07,2021,Leyton Orient,Tranmere Rovers,0,1
2022-05-07,2021,Mansfield Town,Forest Green Rovers,2,2
2022-05-07,2021,Newport County,Rochdale,0,2
2022-05-07,2021,Oldham Athletic,Crawley Town,3,3
2022-05-07,2021,Stevenage Borough,Salford City,4,2
2022-05-07,2021,Walsall,Swindon Town,0,3
```

Create a table:

```sql
CREATE TABLE football
(
    `date` Date,
    `season` Int16,
    `home_team` LowCardinality(String),
    `away_team` LowCardinality(String),
    `home_team_goals` Int8,
    `away_team_goals` Int8
)
ENGINE = MergeTree
ORDER BY (date, home_team);
```

Insert data using the `CSVWithNames` format:

```sql
INSERT INTO football FROM INFILE 'football.csv' FORMAT CSVWithNames;
```

### Reading data {#reading-data}

Read data using the `CSVWithNames` format:

```sql
SELECT *
FROM football
FORMAT CSVWithNames
```

The output will be a CSV with a single header row:

```csv
"date","season","home_team","away_team","home_team_goals","away_team_goals"
"2022-04-30",2021,"Sutton United","Bradford City",1,4
"2022-04-30",2021,"Swindon Town","Barrow",2,1
"2022-04-30",2021,"Tranmere Rovers","Oldham Athletic",2,0
"2022-05-02",2021,"Port Vale","Newport County",1,2
"2022-05-02",2021,"Salford City","Mansfield Town",2,2
"2022-05-07",2021,"Barrow","Northampton Town",1,3
"2022-05-07",2021,"Bradford City","Carlisle United",2,0
"2022-05-07",2021,"Bristol Rovers","Scunthorpe United",7,0
"2022-05-07",2021,"Exeter City","Port Vale",0,1
"2022-05-07",2021,"Harrogate Town A.F.C.","Sutton United",0,2
"2022-05-07",2021,"Hartlepool United","Colchester United",0,2
"2022-05-07",2021,"Leyton Orient","Tranmere Rovers",0,1
"2022-05-07",2021,"Mansfield Town","Forest Green Rovers",2,2
"2022-05-07",2021,"Newport County","Rochdale",0,2
"2022-05-07",2021,"Oldham Athletic","Crawley Town",3,3
"2022-05-07",2021,"Stevenage Borough","Salford City",4,2
"2022-05-07",2021,"Walsall","Swindon Town",0,3
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("CSVWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](../formats/TabSeparatedWithNamesAndTypes).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

:::tip
Starting from [version](https://github.com/ClickHouse/ClickHouse/releases) 23.1, ClickHouse will automatically detect headers in CSV files when using the `CSV` format, so it is not necessary to use `CSVWithNames` or `CSVWithNamesAndTypes`.
:::

Using the following CSV file, named as `football_types.csv`:

```csv
date,season,home_team,away_team,home_team_goals,away_team_goals
Date,Int16,LowCardinality(String),LowCardinality(String),Int8,Int8
2022-04-30,2021,Sutton United,Bradford City,1,4
2022-04-30,2021,Swindon Town,Barrow,2,1
2022-04-30,2021,Tranmere Rovers,Oldham Athletic,2,0
2022-05-02,2021,Salford City,Mansfield Town,2,2
2022-05-02,2021,Port Vale,Newport County,1,2
2022-05-07,2021,Barrow,Northampton Town,1,3
2022-05-07,2021,Bradford City,Carlisle United,2,0
2022-05-07,2021,Bristol Rovers,Scunthorpe United,7,0
2022-05-07,2021,Exeter City,Port Vale,0,1
2022-05-07,2021,Harrogate Town A.F.C.,Sutton United,0,2
2022-05-07,2021,Hartlepool United,Colchester United,0,2
2022-05-07,2021,Leyton Orient,Tranmere Rovers,0,1
2022-05-07,2021,Mansfield Town,Forest Green Rovers,2,2
2022-05-07,2021,Newport County,Rochdale,0,2
2022-05-07,2021,Oldham Athletic,Crawley Town,3,3
2022-05-07,2021,Stevenage Borough,Salford City,4,2
2022-05-07,2021,Walsall,Swindon Town,0,3
```

Create a table:

```sql
CREATE TABLE football
(
    `date` Date,
    `season` Int16,
    `home_team` LowCardinality(String),
    `away_team` LowCardinality(String),
    `home_team_goals` Int8,
    `away_team_goals` Int8
)
ENGINE = MergeTree
ORDER BY (date, home_team);
```

Insert data using the `CSVWithNamesAndTypes` format:

```sql
INSERT INTO football FROM INFILE 'football_types.csv' FORMAT CSVWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `CSVWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT CSVWithNamesAndTypes
```

The output will be a CSV with a two header rows for column names and types:

```csv
"date","season","home_team","away_team","home_team_goals","away_team_goals"
"Date","Int16","LowCardinality(String)","LowCardinality(String)","Int8","Int8"
"2022-04-30",2021,"Sutton United","Bradford City",1,4
"2022-04-30",2021,"Swindon Town","Barrow",2,1
"2022-04-30",2021,"Tranmere Rovers","Oldham Athletic",2,0
"2022-05-02",2021,"Port Vale","Newport County",1,2
"2022-05-02",2021,"Salford City","Mansfield Town",2,2
"2022-05-07",2021,"Barrow","Northampton Town",1,3
"2022-05-07",2021,"Bradford City","Carlisle United",2,0
"2022-05-07",2021,"Bristol Rovers","Scunthorpe United",7,0
"2022-05-07",2021,"Exeter City","Port Vale",0,1
"2022-05-07",2021,"Harrogate Town A.F.C.","Sutton United",0,2
"2022-05-07",2021,"Hartlepool United","Colchester United",0,2
"2022-05-07",2021,"Leyton Orient","Tranmere Rovers",0,1
"2022-05-07",2021,"Mansfield Town","Forest Green Rovers",2,2
"2022-05-07",2021,"Newport County","Rochdale",0,2
"2022-05-07",2021,"Oldham Athletic","Crawley Town",3,3
"2022-05-07",2021,"Stevenage Borough","Salford City",4,2
"2022-05-07",2021,"Walsall","Swindon Town",0,3
```

## Format settings {#format-settings}

:::note
If setting [input_format_with_names_use_header](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::

:::note
If setting [input_format_with_types_use_header](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::
)DOCS_MD"});
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

void registerFileSegmentationEngineCSV(FormatFactory & factory);
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

void registerCSVSchemaReader(FormatFactory & factory);
void registerCSVSchemaReader(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerSchemaReader(format_name, [with_names, with_types](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<CSVSchemaReader>(buf, with_names, with_types, settings);
        });
        factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [with_names](const FormatSettings & settings)
        {
            String result = getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::CSV);
            result += fmt::format(", skip_first_lines={}", settings.csv.skip_first_lines);
            if (!with_names)
                result += fmt::format(", column_names_for_schema_inference={}, try_detect_header={}", settings.column_names_for_schema_inference, settings.csv.try_detect_header);
            return result;
        });
    };

    registerWithNamesAndTypes("CSV", register_func);
}

}
