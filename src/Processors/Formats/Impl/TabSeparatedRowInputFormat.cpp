#include <IO/ReadHelpers.h>
#include <IO/Operators.h>

#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/verbosePrintString.h>
#include <Formats/EscapingRuleUtils.h>
#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/** Check for a common error case - usage of Windows line feed.
  */
static void checkForCarriageReturn(ReadBuffer & in)
{
    if (!in.eof() && (in.position()[0] == '\r' || (in.position() != in.buffer().begin() && in.position()[-1] == '\r')))
        throw Exception(ErrorCodes::INCORRECT_DATA, "\nYou have carriage return (\\r, 0x0D, ASCII 13) at end of first row."
            "\nIt's like your input data has DOS/Windows style line separators, that are illegal in TabSeparated format."
            " You must transform your file to Unix format."
            "\nBut if you really need carriage return at end of string value of last column, you need to escape it as \\r"
            "\nor else enable setting 'input_format_tsv_crlf_end_of_line'");
}

TabSeparatedRowInputFormat::TabSeparatedRowInputFormat(
    SharedHeader header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    bool is_raw_,
    const FormatSettings & format_settings_)
    : TabSeparatedRowInputFormat(header_, std::make_unique<PeekableReadBuffer>(in_), params_, with_names_, with_types_, is_raw_, format_settings_)
{
}

TabSeparatedRowInputFormat::TabSeparatedRowInputFormat(
    SharedHeader header_,
    std::unique_ptr<PeekableReadBuffer> in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    bool is_raw,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        *in_,
        params_,
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<TabSeparatedFormatReader>(*in_, format_settings_, is_raw),
        format_settings_.tsv.try_detect_header,
        format_settings_.tsv.allow_variable_number_of_columns)
    , buf(std::move(in_))
{
}

void TabSeparatedRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    RowInputFormatWithNamesAndTypes::setReadBuffer(*buf);
}

void TabSeparatedRowInputFormat::resetReadBuffer()
{
    buf.reset();
    RowInputFormatWithNamesAndTypes::resetReadBuffer();
}

TabSeparatedFormatReader::TabSeparatedFormatReader(PeekableReadBuffer & in_, const FormatSettings & format_settings_, bool is_raw_)
    : FormatWithNamesAndTypesReader(in_, format_settings_), buf(&in_), is_raw(is_raw_)
{
}

void TabSeparatedFormatReader::skipFieldDelimiter()
{
    assertChar('\t', *buf);
}

void TabSeparatedFormatReader::skipRowEndDelimiter()
{
    if (buf->eof())
        return;

    if (format_settings.tsv.crlf_end_of_line_input)
    {
        if (*buf->position() == '\r')
            ++buf->position();
    }
    else if (unlikely(first_row))
    {
        checkForCarriageReturn(*buf);
        first_row = false;
    }

    assertChar('\n', *buf);
}

template <bool read_string>
String TabSeparatedFormatReader::readFieldIntoString()
{
    String field;
    bool support_crlf = format_settings.tsv.crlf_end_of_line_input;
    if (is_raw)
        readString(field, *buf);
    else
    {
        if constexpr (read_string)
            support_crlf ? readEscapedStringCRLF(field, *buf) : readEscapedString(field, *buf);
        else
            support_crlf ? readTSVFieldCRLF(field, *buf) : readTSVField(field, *buf);
    }
    return field;
}

void TabSeparatedFormatReader::skipField()
{
    NullOutput out;
    if (is_raw)
        readStringInto(out, *buf);
    else
        format_settings.tsv.crlf_end_of_line_input ? readEscapedStringInto<NullOutput,true>(out, *buf) : readEscapedStringInto<NullOutput,false>(out, *buf);
}

void TabSeparatedFormatReader::skipHeaderRow()
{
    do
    {
        skipField();
    }
    while (checkChar('\t', *buf));

    skipRowEndDelimiter();
}

template <bool is_header_row>
std::vector<String> TabSeparatedFormatReader::readRowImpl()
{
    std::vector<String> fields;
    do
    {
        fields.push_back(readFieldIntoString<is_header_row>());
    }
    while (checkChar('\t', *buf));

    skipRowEndDelimiter();
    return fields;
}

bool TabSeparatedFormatReader::readField(IColumn & column, const DataTypePtr & type,
    const SerializationPtr & serialization, bool is_last_file_column, const String & /*column_name*/)
{
    const bool at_delimiter = !is_last_file_column && !buf->eof() && *buf->position() == '\t';
    const bool at_last_column_line_end = is_last_file_column && (buf->eof() || *buf->position() == '\n' || (format_settings.tsv.crlf_end_of_line_input && *buf->position() == '\r'));

    if (format_settings.tsv.empty_as_default && (at_delimiter || at_last_column_line_end))
    {
        column.insertDefault();
        return false;
    }

    bool as_nullable = format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type);

    if (is_raw)
    {
        if (as_nullable)
            return SerializationNullable::deserializeNullAsDefaultOrNestedTextRaw(column, *buf, format_settings, serialization);

        serialization->deserializeTextRaw(column, *buf, format_settings);
        return true;
    }


    if (as_nullable)
        return SerializationNullable::deserializeNullAsDefaultOrNestedTextEscaped(column, *buf, format_settings, serialization);

    serialization->deserializeTextEscaped(column, *buf, format_settings);
    return true;
}

bool TabSeparatedFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    try
    {
        assertChar('\t', *buf);
    }
    catch (const DB::Exception &)
    {
        if (*buf->position() == '\n')
        {
            out << "ERROR: Line feed found where tab is expected."
                   " It's like your file has less columns than expected.\n"
                   "And if your file has the right number of columns, "
                   "maybe it has an unescaped backslash in value before tab, which causes the tab to be escaped.\n";
        }
        else if (*buf->position() == '\r')
        {
            out << "ERROR: Carriage return found where tab is expected.\n";
        }
        else
        {
            out << "ERROR: There is no tab. ";
            verbosePrintString(buf->position(), buf->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

bool TabSeparatedFormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    if (buf->eof())
        return true;

    try
    {
        if (!format_settings.tsv.crlf_end_of_line_input)
            assertChar('\n', *buf);
        else
            assertChar('\r', *buf);
    }
    catch (const DB::Exception &)
    {
        if (*buf->position() == '\t')
        {
            out << "ERROR: Tab found where line feed is expected."
                   " It's like your file has more columns than expected.\n"
                   "And if your file has the right number of columns, maybe it has an unescaped tab in a value.\n";
        }
        else if (*buf->position() == '\r')
        {
            out << "ERROR: Carriage return found where line feed is expected."
                   " It's like your file has DOS/Windows style line separators. \n"
                   "You must transform your file to Unix format. \n"
                   "But if you really need carriage return at end of string value of last column, you need to escape it as \\r \n"
                   "or else enable setting 'input_format_tsv_crlf_end_of_line'";
        }
        else
        {
            out << "ERROR: There is no line feed. ";
            verbosePrintString(buf->position(), buf->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

void TabSeparatedFormatReader::checkNullValueForNonNullable(DataTypePtr type)
{
    bool can_be_parsed_as_null = isNullableOrLowCardinalityNullable(type) || format_settings.null_as_default;

    // check null value for type is not nullable. don't cross buffer bound for simplicity, so maybe missing some case
    if (!can_be_parsed_as_null && !buf->eof())
    {
        if (*buf->position() == '\\' && buf->available() >= 2)
        {
            ++buf->position();
            if (*buf->position() == 'N')
            {
                ++buf->position();
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected NULL value of not Nullable type {}", type->getName());
            }

            --buf->position();
        }
    }
}

void TabSeparatedFormatReader::skipPrefixBeforeHeader()
{
    for (size_t i = 0; i != format_settings.tsv.skip_first_lines; ++i)
    {
        if (buf->eof())
            break;
        readRow();
    }
}

void TabSeparatedRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*buf);
}

void TabSeparatedFormatReader::setReadBuffer(ReadBuffer & in_)
{
    buf = assert_cast<PeekableReadBuffer *>(&in_);
    FormatWithNamesAndTypesReader::setReadBuffer(*buf);
}

bool TabSeparatedFormatReader::checkForSuffix()
{
    if (!format_settings.tsv.skip_trailing_empty_lines)
        return buf->eof();

    PeekableReadBufferCheckpoint checkpoint(*buf);
    while (checkChar('\n', *buf) || checkChar('\r', *buf));
    if (buf->eof())
        return true;

    buf->rollbackToCheckpoint();
    return false;
}

void TabSeparatedFormatReader::skipRow()
{
    ReadBuffer & istr = *buf;
    while (!istr.eof())
    {
        char * pos = nullptr;
        if (is_raw)
            pos = find_first_symbols<'\r', '\n'>(istr.position(), istr.buffer().end());
        else
            pos = find_first_symbols<'\\', '\r', '\n'>(istr.position(), istr.buffer().end());

        istr.position() = pos;

        if (istr.position() > istr.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
        if (pos == istr.buffer().end())
            continue;

        if (!is_raw && *istr.position() == '\\')
        {
            ++istr.position();
            if (!istr.eof())
                ++istr.position();
            continue;
        }

        if (*istr.position() == '\n')
        {
            ++istr.position();
            if (!istr.eof() && *istr.position() == '\r')
                ++istr.position();
            return;
        }
        if (*istr.position() == '\r')
        {
            ++istr.position();
            if (!istr.eof() && *istr.position() == '\n')
            {
                ++istr.position();
                return;
            }
        }
    }
}

bool TabSeparatedFormatReader::checkForEndOfRow()
{
    return buf->eof() || *buf->position() == '\n' || (format_settings.tsv.crlf_end_of_line_input && *buf->position() == '\r');
}

TabSeparatedSchemaReader::TabSeparatedSchemaReader(
    ReadBuffer & in_, bool with_names_, bool with_types_, bool is_raw_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(
        buf,
        format_settings_,
        with_names_,
        with_types_,
        &reader,
        getDefaultDataTypeForEscapingRule(is_raw_ ? FormatSettings::EscapingRule::Raw : FormatSettings::EscapingRule::Escaped),
        format_settings_.tsv.try_detect_header)
    , buf(in_)
    , reader(buf, format_settings_, is_raw_)
{
}

std::optional<std::pair<std::vector<String>, DataTypes>> TabSeparatedSchemaReader::readRowAndGetFieldsAndDataTypes()
{
    if (buf.eof())
        return {};

    auto fields = reader.readRow();
    auto data_types = tryInferDataTypesByEscapingRule(fields, reader.getFormatSettings(), reader.getEscapingRule());
    return std::make_pair(fields, data_types);
}

std::optional<DataTypes> TabSeparatedSchemaReader::readRowAndGetDataTypesImpl()
{
    auto fields_with_types = readRowAndGetFieldsAndDataTypes();
    if (!fields_with_types)
        return {};
    return std::move(fields_with_types->second);
}

void registerInputFormatTabSeparated(FormatFactory & factory);
void registerInputFormatTabSeparated(FormatFactory & factory)
{
    for (bool is_raw : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerInputFormat(format_name, [with_names, with_types, is_raw](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
            {
                return std::make_shared<TabSeparatedRowInputFormat>(std::make_shared<const Block>(sample), buf, std::move(params), with_names, with_types, is_raw, settings);
            });
        };

        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
        if (is_raw)
            registerWithNamesAndTypes("Raw", register_func);
    }

    factory.setDocumentation("Raw", Documentation{
        .description = "An alias for the `TabSeparatedRaw` format. See the `TabSeparatedRaw` entry for the full documentation.",
        .related = {"TabSeparatedRaw"}});

    factory.setDocumentation("RawWithNames", Documentation{
        .description = "An alias for the `TabSeparatedRawWithNames` format. See the `TabSeparatedRawWithNames` entry for the full documentation.",
        .related = {"TabSeparatedRawWithNames"}});

    factory.setDocumentation("RawWithNamesAndTypes", Documentation{
        .description = "An alias for the `TabSeparatedRawWithNamesAndTypes` format. See the `TabSeparatedRawWithNamesAndTypes` entry for the full documentation.",
        .related = {"TabSeparatedRawWithNamesAndTypes"}});

    factory.setDocumentation("TSV", Documentation{
        .description = "An alias for the `TabSeparated` format. See the `TabSeparated` entry for the full documentation.",
        .related = {"TabSeparated"}});

    factory.setDocumentation("TSVRaw", Documentation{
        .description = "An alias for the `TabSeparatedRaw` format. See the `TabSeparatedRaw` entry for the full documentation.",
        .related = {"TabSeparatedRaw"}});

    factory.setDocumentation("TSVRawWithNames", Documentation{
        .description = "An alias for the `TabSeparatedRawWithNames` format. See the `TabSeparatedRawWithNames` entry for the full documentation.",
        .related = {"TabSeparatedRawWithNames"}});

    factory.setDocumentation("TSVRawWithNamesAndTypes", Documentation{
        .description = "An alias for the `TabSeparatedRawWithNamesAndTypes` format. See the `TabSeparatedRawWithNamesAndTypes` entry for the full documentation.",
        .related = {"TabSeparatedRawWithNamesAndTypes"}});

    factory.setDocumentation("TSVWithNames", Documentation{
        .description = "An alias for the `TabSeparatedWithNames` format. See the `TabSeparatedWithNames` entry for the full documentation.",
        .related = {"TabSeparatedWithNames"}});

    factory.setDocumentation("TSVWithNamesAndTypes", Documentation{
        .description = "An alias for the `TabSeparatedWithNamesAndTypes` format. See the `TabSeparatedWithNamesAndTypes` entry for the full documentation.",
        .related = {"TabSeparatedWithNamesAndTypes"}});

    factory.setDocumentation("TabSeparated", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias  |
|-------|--------|--------|
| ✔     | ✔      | `TSV`  |

## Description {#description}

In TabSeparated format, data is written by row. Each row contains values separated by tabs. Each value is followed by a tab, except the last value in the row, which is followed by a line feed. Strictly Unix line feeds are assumed everywhere. The last row also must contain a line feed at the end. Values are written in text format, without enclosing quotation marks, and with special characters escaped.

This format is also available under the name `TSV`.

The `TabSeparated` format is convenient for processing data using custom programs and scripts. It is used by default in the HTTP interface, and in the command-line client's batch mode. This format also allows transferring data between different DBMSs. For example, you can get a dump from MySQL and upload it to ClickHouse, or vice versa.

The `TabSeparated` format supports outputting total values (when using WITH TOTALS) and extreme values (when 'extremes' is set to 1). In these cases, the total values and extremes are output after the main data. The main result, total values, and extremes are separated from each other by an empty line. Example:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

## Data formatting {#tabseparated-data-formatting}

Integer numbers are written in decimal form. Numbers can contain an extra "+" character at the beginning (ignored when parsing, and not recorded when formatting). Non-negative numbers can't contain the negative sign. When reading, it is allowed to parse an empty string as a zero, or (for signed types) a string consisting of just a minus sign as a zero. Numbers that do not fit into the corresponding data type may be parsed as a different number, without an error message.

Floating-point numbers are written in decimal form. The dot is used as the decimal separator. Exponential entries are supported, as are 'inf', '+inf', '-inf', and 'nan'. An entry of floating-point numbers may begin or end with a decimal point.
During formatting, accuracy may be lost on floating-point numbers.
During parsing, it is not strictly required to read the nearest machine-representable number.

Dates are written in YYYY-MM-DD format and parsed in the same format, but with any characters as separators.
Dates with times are written in the format `YYYY-MM-DD hh:mm:ss` and parsed in the same format, but with any characters as separators.
This all occurs in the system time zone at the time the client or server starts (depending on which of them formats data). For dates with times, daylight saving time is not specified. So if a dump has times during daylight saving time, the dump does not unequivocally match the data, and parsing will select one of the two times.
During a read operation, incorrect dates and dates with times can be parsed with natural overflow or as null dates and times, without an error message.

As an exception, parsing dates with times is also supported in Unix timestamp format, if it consists of exactly 10 decimal digits. The result is not time zone-dependent. The formats `YYYY-MM-DD hh:mm:ss` and `NNNNNNNNNN` are differentiated automatically.

Strings are output with backslash-escaped special characters. The following escape sequences are used for output: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. Parsing also supports the sequences `\a`, `\v`, and `\xHH` (hex escape sequences) and any `\c` sequences, where `c` is any character (these sequences are converted to `c`). Thus, reading data supports formats where a line feed can be written as `\n` or `\`, or as a line feed. For example, the string `Hello world` with a line feed between the words instead of space can be parsed in any of the following variations:

```text
Hello\nworld

Hello\
world
```

The second variant is supported because MySQL uses it when writing tab-separated dumps.

The minimum set of characters that you need to escape when passing data in TabSeparated format: tab, line feed (LF) and backslash.

Only a small set of symbols are escaped. You can easily stumble onto a string value that your terminal will ruin in output.

Arrays are written as a list of comma-separated values in `[]`. Number items in the array are formatted as normally. `Date` and `DateTime` types are written in single quotes. Strings are written in single quotes with the same escaping rules as above.

[NULL](/sql-reference/syntax.md) is formatted according to setting [format_tsv_null_representation](/operations/settings/settings-formats.md/#format_tsv_null_representation) (default value is `\N`).

In input data, ENUM values can be represented as names or as ids. First, we try to match the input value to the ENUM name. If we fail and the input value is a number, we try to match this number to ENUM id.
If input data contains only ENUM ids, it's recommended to enable the setting [input_format_tsv_enum_as_number](/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) to optimize ENUM parsing.

Each element of [Nested](/sql-reference/data-types/nested-data-structures/index.md) structures is represented as an array.

For example:

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```
```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```
```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

### Reading data {#reading-data}

Read data using the `TabSeparated` format:

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

The output will be in tab separated format:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}

| Setting                                                                                                                                                          | Description                                                                                                                                                                                                                                    | Default |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| [`format_tsv_null_representation`](/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | Custom NULL representation in TSV format.                                                                                                                                                                                                      | `\N`    |
| [`input_format_tsv_empty_as_default`](/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | treat empty fields in TSV input as default values. For complex default expressions [input_format_defaults_for_omitted_fields](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too. | `false` |
| [`input_format_tsv_enum_as_number`](/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | treat inserted enum values in TSV formats as enum indices.                                                                                                                                                                                     | `false` |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | use some tweaks and heuristics to infer schema in TSV format. If disabled, all fields will be inferred as Strings.                                                                                                                             | `true`  |
| [`output_format_tsv_crlf_end_of_line`](/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | if it is set true, end of line in TSV output format will be `\r\n` instead of `\n`.                                                                                                                                                            | `false` |
| [`input_format_tsv_crlf_end_of_line`](/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | if it is set true, end of line in TSV input format will be `\r\n` instead of `\n`.                                                                                                                                                             | `false` |
| [`input_format_tsv_skip_first_lines`](/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | skip specified number of lines at the beginning of data.                                                                                                                                                                                       | `0`     |
| [`input_format_tsv_detect_header`](/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | automatically detect header with names and types in TSV format.                                                                                                                                                                                | `true`  |
| [`input_format_tsv_skip_trailing_empty_lines`](/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | skip trailing empty lines at the end of data.                                                                                                                                                                                                  | `false` |
| [`input_format_tsv_allow_variable_number_of_columns`](/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | allow variable number of columns in TSV format, ignore extra columns and use default values on missing columns.                                                                                                                                | `false` |
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedRaw", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias           |
|-------|--------|-----------------|
| ✔     | ✔      | `TSVRaw`, `Raw` |

## Description {#description}

Differs from the [`TabSeparated`](/interfaces/formats/TabSeparated) format in that rows are written without escaping.

:::note
When parsing with this format, tabs or line-feeds are not allowed in each field.
:::

For a comparison of the `TabSeparatedRaw` format and the `RawBlob` format see: [Raw Formats Comparison](../RawBLOB.md/#raw-formats-comparison)

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRaw;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedRaw` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRaw
```

The output will be in tab separated format:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedRawWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                             |
|-------|--------|-----------------------------------|
| ✔     | ✔      | `TSVRawWithNames`, `RawWithNames` |

## Description {#description}

Differs from the [`TabSeparatedWithNames`](./TabSeparatedWithNames.md) format, 
in that the rows are written without escaping.

:::note
When parsing with this format, tabs or line-feeds are not allowed in each field.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRawWithNames;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedRawWithNames` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRawWithNames
```

The output will be in tab separated format with a single line header:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedRawWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                                             |
|-------|--------|---------------------------------------------------|
| ✔     | ✔      | `TSVRawWithNamesAndTypes`, `RawWithNamesAndTypes` |

## Description {#description}

Differs from the [`TabSeparatedWithNamesAndTypes`](./TabSeparatedWithNamesAndTypes.md) format,
in that the rows are written without escaping.

:::note
When parsing with this format, tabs or line-feeds are not allowed in each field.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
Date    Int16   LowCardinality(String)  LowCardinality(String)  Int8    Int8
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRawWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedRawWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRawWithNamesAndTypes
```

The output will be in tab separated format with two header rows for column names and types:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
Date    Int16   LowCardinality(String)  LowCardinality(String)  Int8    Int8
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                          |
|-------|--------|--------------------------------|
|     ✔    |     ✔     | `TSVWithNames` |

## Description {#description}

Differs from the [`TabSeparated`](./TabSeparated.md) format in that the column names are written in the first row.

During parsing, the first row is expected to contain the column names. You can use column names to determine their position and to check their correctness.

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns of the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedWithNames;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedWithNames` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedWithNames
```

The output will be in tab separated format:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                                          |
|-------|--------|------------------------------------------------|
|     ✔    |     ✔     | `TSVWithNamesAndTypes` |

## Description {#description}

Differs from the [`TabSeparated`](./TabSeparated.md) format in that the column names are written to the first row, while the column types are in the second row.

:::note
- If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns in the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
- If setting [`input_format_with_types_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
Date    Int16   LowCardinality(String)  LowCardinality(String)  Int8    Int8
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedWithNamesAndTypes
```

The output will be in tab separated format with two header rows for column names and types:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
Date    Int16   LowCardinality(String)  LowCardinality(String)  Int8    Int8
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerTSVSchemaReader(FormatFactory & factory);
void registerTSVSchemaReader(FormatFactory & factory)
{
    for (bool is_raw : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerSchemaReader(format_name, [with_names, with_types, is_raw](ReadBuffer & buf, const FormatSettings & settings)
            {
                return std::make_shared<TabSeparatedSchemaReader>(buf, with_names, with_types, is_raw, settings);
            });
            factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [with_names, is_raw](const FormatSettings & settings)
            {
                String result = getAdditionalFormatInfoByEscapingRule(
                    settings, is_raw ? FormatSettings::EscapingRule::Raw : FormatSettings::EscapingRule::Escaped);
                result += fmt::format(", skip_first_lines={}", settings.tsv.skip_first_lines);
                if (!with_names)
                    result += fmt::format(
                        ", column_names_for_schema_inference={}, try_detect_header={}",
                        settings.column_names_for_schema_inference,
                        settings.tsv.try_detect_header);
                return result;
            });
        };

        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
        if (is_raw)
            registerWithNamesAndTypes("Raw", register_func);
    }
}

static std::pair<bool, size_t> fileSegmentationEngineTabSeparatedImpl(ReadBuffer & in, DB::Memory<> & memory, bool is_raw, size_t min_bytes, size_t min_rows, size_t max_rows)
{
    bool need_more_data = true;
    char * pos = in.position();
    size_t number_of_rows = 0;

    if (max_rows && (max_rows < min_rows))
        max_rows = min_rows;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        if (is_raw)
            pos = find_first_symbols<'\r', '\n'>(pos, in.buffer().end());
        else
            pos = find_first_symbols<'\\', '\r', '\n'>(pos, in.buffer().end());

        if (pos > in.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
        if (pos == in.buffer().end())
            continue;

        if (!is_raw && *pos == '\\')
        {
            ++pos;
            if (loadAtPosition(in, memory, pos))
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

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineTabSeparated(FormatFactory & factory);
void registerFileSegmentationEngineTabSeparated(FormatFactory & factory)
{
    for (bool is_raw : {false, true})
    {
        auto register_func = [&](const String & format_name, bool, bool)
        {
            static constexpr size_t min_rows = 3; /// Make it 3 for header auto detection (first 3 rows must be always in the same segment).
            factory.registerFileSegmentationEngine(format_name, [is_raw](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
            {
                return fileSegmentationEngineTabSeparatedImpl(in, memory, is_raw, min_bytes, min_rows, max_rows);
            });
        };

        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        if (is_raw)
            registerWithNamesAndTypes("Raw", register_func);
        markFormatWithNamesAndTypesSupportsSamplingColumns(is_raw ? "TSVRaw" : "TSV", factory);
        markFormatWithNamesAndTypesSupportsSamplingColumns(is_raw ? "TabSeparatedRaw" : "TabSeparated", factory);
        if (is_raw)
            markFormatWithNamesAndTypesSupportsSamplingColumns("Raw", factory);
    }

    // We can use the same segmentation engine for TSKV.
    factory.registerFileSegmentationEngine("TSKV", [](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
    {
        return fileSegmentationEngineTabSeparatedImpl(in, memory, false, min_bytes, 1, max_rows);
    });
}

}
