#include <Processors/Formats/Impl/JSONCompactEachRowRowInputFormat.h>

#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Formats/FormatFactory.h>
#include <Formats/verbosePrintString.h>
#include <Formats/JSONUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

JSONCompactEachRowRowInputFormat::JSONCompactEachRowRowInputFormat(
    SharedHeader header_,
    ReadBuffer & in_,
    Params params_,
    bool with_names_,
    bool with_types_,
    bool yield_strings_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        in_,
        params_,
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<JSONCompactEachRowFormatReader>(in_, yield_strings_, format_settings_),
        false,
        format_settings_.json.compact_allow_variable_number_of_columns)
{
}

void JSONCompactEachRowRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

JSONCompactEachRowFormatReader::JSONCompactEachRowFormatReader(ReadBuffer & in_, bool yield_strings_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesReader(in_, format_settings_), yield_strings(yield_strings_)
{
}

void JSONCompactEachRowFormatReader::skipRowStartDelimiter()
{
    JSONUtils::skipArrayStart(*in);
}

void JSONCompactEachRowFormatReader::skipFieldDelimiter()
{
    JSONUtils::skipComma(*in);
}

void JSONCompactEachRowFormatReader::skipRowEndDelimiter()
{
    skipWhitespaceIfAny(*in);
    JSONUtils::skipArrayEnd(*in);
}

void JSONCompactEachRowFormatReader::skipRowBetweenDelimiter()
{
    skipWhitespaceIfAny(*in);
    if (!in->eof() && (*in->position() == ',' || *in->position() == ';'))
        ++in->position();
    skipWhitespaceIfAny(*in);
}

void JSONCompactEachRowFormatReader::skipField()
{
    skipWhitespaceIfAny(*in);
    skipJSONField(*in, "skipped_field", format_settings.json);
}

void JSONCompactEachRowFormatReader::skipHeaderRow()
{
    skipRowStartDelimiter();
    do
    {
        skipField();
        skipWhitespaceIfAny(*in);
    }
    while (checkChar(',', *in));

    skipRowEndDelimiter();
}

bool JSONCompactEachRowFormatReader::checkForSuffix()
{
    skipWhitespaceIfAny(*in);
    /// Allow ',' and ';' after the last row.
    if (!in->eof() && (*in->position() == ',' || *in->position() == ';'))
        ++in->position();
    skipWhitespaceIfAny(*in);
    return in->eof();
}

void JSONCompactEachRowFormatReader::skipRow()
{
    JSONUtils::skipRowForJSONCompactEachRow(*in);
}

std::vector<String> JSONCompactEachRowFormatReader::readHeaderRow()
{
    skipRowStartDelimiter();
    std::vector<String> fields;
    String field;
    do
    {
        skipWhitespaceIfAny(*in);
        readJSONString(field, *in, format_settings.json);
        fields.push_back(field);
        skipWhitespaceIfAny(*in);
    }
    while (checkChar(',', *in));

    skipRowEndDelimiter();
    return fields;
}

bool JSONCompactEachRowFormatReader::readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool /*is_last_file_column*/, const String & column_name)
{
    skipWhitespaceIfAny(*in);
    return JSONUtils::readField(*in, column, type, serialization, column_name, format_settings, yield_strings);
}

bool JSONCompactEachRowFormatReader::checkForEndOfRow()
{
    skipWhitespaceIfAny(*in);
    return !in->eof() && *in->position() == ']';
}

bool JSONCompactEachRowFormatReader::parseRowStartWithDiagnosticInfo(WriteBuffer & out)
{
    skipWhitespaceIfAny(*in);
    if (!checkChar('[', *in))
    {
        out << "ERROR: There is no '[' before the row.\n";
        return false;
    }

    return true;
}

bool JSONCompactEachRowFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    try
    {
        JSONUtils::skipComma(*in);
    }
    catch (const DB::Exception &)
    {
        if (*in->position() == ']')
        {
            out << "ERROR: Closing parenthesis (']') found where comma is expected."
                   " It's like your file has less columns than expected.\n"
                   "And if your file has the right number of columns, maybe it has unescaped quotes in values.\n";
        }
        else
        {
            out << "ERROR: There is no comma. ";
            verbosePrintString(in->position(), in->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

bool JSONCompactEachRowFormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    skipWhitespaceIfAny(*in);

    if (in->eof())
    {
        out << "ERROR: Unexpected end of file. ']' expected at the end of row.";
        return false;
    }

    if (!checkChar(']', *in))
    {
        out << "ERROR: There is no closing parenthesis (']') at the end of the row. ";
        verbosePrintString(in->position(), in->position() + 1, out);
        out << " found instead.\n";
        return false;
    }

    skipWhitespaceIfAny(*in);

    if (in->eof())
        return true;

    if ((*in->position() == ',' || *in->position() == ';'))
        ++in->position();

    skipWhitespaceIfAny(*in);
    return true;
}

JSONCompactEachRowRowSchemaReader::JSONCompactEachRowRowSchemaReader(
    ReadBuffer & in_, bool with_names_, bool with_types_, bool yield_strings_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(in_, format_settings_, with_names_, with_types_, &reader)
    , reader(in_, yield_strings_, format_settings_)
{
}

std::optional<DataTypes> JSONCompactEachRowRowSchemaReader::readRowAndGetDataTypesImpl()
{
    if (first_row)
        first_row = false;
    else
    {
        skipWhitespaceIfAny(in);
        /// ',' and ';' are possible between the rows.
        if (!in.eof() && (*in.position() == ',' || *in.position() == ';'))
            ++in.position();
    }

    skipWhitespaceIfAny(in);
    if (in.eof())
        return {};

    return JSONUtils::readRowAndGetDataTypesForJSONCompactEachRow(in, format_settings, &inference_info);
}

void JSONCompactEachRowRowSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredJSONTypesIfNeeded(type, new_type, format_settings, &inference_info);
}

void JSONCompactEachRowRowSchemaReader::transformTypesFromDifferentFilesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredJSONTypesFromDifferentFilesIfNeeded(type, new_type, format_settings);
}

void JSONCompactEachRowRowSchemaReader::transformFinalTypeIfNeeded(DataTypePtr & type)
{
    transformFinalInferredJSONTypeIfNeeded(type, format_settings, &inference_info);
}

void registerInputFormatJSONCompactEachRow(FormatFactory & factory);
void registerInputFormatJSONCompactEachRow(FormatFactory & factory)
{
    for (bool yield_strings : {true, false})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerInputFormat(format_name, [with_names, with_types, yield_strings](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
            {
                return std::make_shared<JSONCompactEachRowRowInputFormat>(std::make_unique<const Block>(sample), buf, std::move(params), with_names, with_types, yield_strings, settings);
            });
        };

        registerWithNamesAndTypes(yield_strings ? "JSONCompactStringsEachRow" : "JSONCompactEachRow", register_func);
        markFormatWithNamesAndTypesSupportsSamplingColumns(yield_strings ? "JSONCompactStringsEachRow" : "JSONCompactEachRow", factory);
    }

    factory.setDocumentation("JSONCompactEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from [`JSONEachRow`](./JSONEachRow.md) only in that data rows are output as arrays, not as objects.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactEachRow;
```

### Reading data {#reading-data}

Read data using the `JSONCompactEachRow` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactEachRow
```

The output will be in JSON format:

```json
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactEachRowWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONCompactEachRow`](./JSONCompactEachRow.md) format in that it also prints the header row with column names, similar to the [`TabSeparatedWithNames`](../TabSeparated/TabSeparatedWithNames.md) format.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactEachRowWithNames;
```

### Reading data {#reading-data}

Read data using the `JSONCompactEachRowWithNames` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactEachRowWithNames
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("JSONCompactEachRowWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONCompactEachRow`](./JSONCompactEachRow.md) format in that it also prints two header rows with column names and types, similar to the [TabSeparatedWithNamesAndTypes](../TabSeparated/TabSeparatedWithNamesAndTypes.md) format.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactEachRowWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `JSONCompactEachRowWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactEachRowWithNamesAndTypes
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [`input_format_with_types_use_header`](/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStringsEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from [`JSONCompactEachRow`](./JSONCompactEachRow.md) only in that data fields are output as strings, not as typed JSON values.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactStringsEachRow;
```

### Reading data {#reading-data}

Read data using the `JSONCompactStringsEachRow` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactStringsEachRow
```

The output will be in JSON format:

```json
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStringsEachRowWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONCompactEachRow`](./JSONCompactEachRow.md) format in that it also prints the header row with column names, similar to the [TabSeparatedWithNames](../TabSeparated/TabSeparatedWithNames.md) format.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactStringsEachRowWithNames;
```

### Reading data {#reading-data}

Read data using the `JSONCompactStringsEachRowWithNames` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactStringsEachRowWithNames
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStringsEachRowWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from `JSONCompactEachRow` format in that it also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](/interfaces/formats/TabSeparatedRawWithNamesAndTypes).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactStringsEachRowWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `JSONCompactStringsEachRowWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactStringsEachRowWithNamesAndTypes
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

## Format settings {#format-settings}

:::note
If setting [input_format_with_names_use_header](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

:::note
If setting [input_format_with_types_use_header](/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::
)DOCS_MD"});
}

void registerJSONCompactEachRowSchemaReader(FormatFactory & factory);
void registerJSONCompactEachRowSchemaReader(FormatFactory & factory)
{
    for (bool json_strings : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerSchemaReader(format_name, [=](ReadBuffer & buf, const FormatSettings & settings)
            {
                return std::make_shared<JSONCompactEachRowRowSchemaReader>(buf, with_names, with_types, json_strings, settings);
            });
            if (!with_types)
            {
                factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [with_names](const FormatSettings & settings)
                {
                    auto result = getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
                    if (!with_names)
                        result += fmt::format(", column_names_for_schema_inference={}", settings.column_names_for_schema_inference);
                    return result;
                });
            }
        };
        registerWithNamesAndTypes(json_strings ? "JSONCompactStringsEachRow" : "JSONCompactEachRow", register_func);
    }
}

void registerFileSegmentationEngineJSONCompactEachRow(FormatFactory & factory);
void registerFileSegmentationEngineJSONCompactEachRow(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        /// In case when we have names and/or types in the first two/one rows,
        /// we need to read at least one more row of actual data. So, set
        /// the minimum of rows for segmentation engine according to
        /// parameters with_names and with_types.
        size_t min_rows = 1 + int(with_names) + int(with_types);
        factory.registerFileSegmentationEngine(format_name, [min_rows](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
        {
            return JSONUtils::fileSegmentationEngineJSONCompactEachRow(in, memory, min_bytes, min_rows, max_rows);
        });
    };

    registerWithNamesAndTypes("JSONCompactEachRow", register_func);
    registerWithNamesAndTypes("JSONCompactStringsEachRow", register_func);
}

}
