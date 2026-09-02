#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <Core/Block.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat(
    WriteBuffer & out_,
    SharedHeader header_,
    const FormatSettings & settings_,
    bool pretty_json_,
    bool with_names_,
    bool with_types_)
    : RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>(
        header_, out_, settings_.json.valid_output_on_exception, settings_.json.validate_utf8)
    , pretty_json(pretty_json_)
    , with_names(with_names_)
    , with_types(with_types_)
    , settings(settings_)
{
    /// The header rows are written before the rows, so they cannot be part of the enclosing array.
    if (settings.json.array_of_rows && (with_names || with_types))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting 'output_format_json_array_of_rows' is not applicable to the 'WithNames' and "
            "'WithNamesAndTypes' variants of the 'JSONEachRow' formats, "
            "because a JSON array cannot contain the header rows");

    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
    fields = JSONUtils::makeNamesValidJSONStrings(getPort(PortKind::Main).getHeader().getNames(), settings, settings.json.validate_utf8);
}


void JSONEachRowRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    JSONUtils::writeFieldFromColumn(column, serialization, row_num, settings.json.serialize_as_strings, settings, *ostr, fields[field_number], pretty_json ? 1 : 0, pretty_json ? " " : "", pretty_json);
    ++field_number;
}


void JSONEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeChar(',', *ostr);
    if (pretty_json)
        writeChar('\n', *ostr);
}


void JSONEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('{', *ostr);
    if (pretty_json)
        writeChar('\n', *ostr);
}


void JSONEachRowRowOutputFormat::writeRowEndDelimiter()
{
    if (pretty_json)
        writeChar('\n', *ostr);

    if (settings.json.array_of_rows)
        writeChar('}', *ostr);
    else
        writeCString("}\n", *ostr);
    field_number = 0;
}


void JSONEachRowRowOutputFormat::writeRowBetweenDelimiter()
{
    if (settings.json.array_of_rows)
        writeCString(",\n", *ostr);
}


void JSONEachRowRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();

    /// The header rows are JSON arrays, written exactly like the `JSONCompactEachRowWithNames*` ones.
    if (with_names)
        JSONUtils::writeStringFieldsAsJSONArrayRow(
            JSONUtils::makeNamesValidJSONStrings(header.getNames(), settings, settings.json.validate_utf8), *ostr);

    if (with_types)
        JSONUtils::writeStringFieldsAsJSONArrayRow(
            JSONUtils::makeNamesValidJSONStrings(header.getDataTypeNames(), settings, settings.json.validate_utf8), *ostr);

    if (settings.json.array_of_rows)
    {
        writeCString("[\n", *ostr);
    }
}


void JSONEachRowRowOutputFormat::writeSuffix()
{
    if (!exception_message.empty())
    {
        if (haveWrittenData())
            writeRowBetweenDelimiter();
        writeRowStartDelimiter();
        JSONUtils::writeException(exception_message, *ostr, settings, pretty_json ? 1 : 0);
        writeRowEndDelimiter();
    }

    if (settings.json.array_of_rows)
        writeCString("\n]\n", *ostr);
}

void JSONEachRowRowOutputFormat::resetFormatterImpl()
{
    RowOutputFormatWithExceptionHandlerAdaptor::resetFormatterImpl();
    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
}

void registerOutputFormatJSONEachRow(FormatFactory & factory);
void registerOutputFormatJSONEachRow(FormatFactory & factory)
{
    auto register_function = [&](const String & format, bool serialize_as_strings, bool pretty_json, bool with_names, bool with_types)
    {
        factory.registerOutputFormat(format, [serialize_as_strings, pretty_json, with_names, with_types](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings,
            FormatFilterInfoPtr /*format_filter_info*/)
        {
            FormatSettings settings = _format_settings;
            settings.json.serialize_as_strings = serialize_as_strings;
            return std::make_shared<JSONEachRowRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings, pretty_json, with_names, with_types);
        });
        factory.markOutputFormatSupportsParallelFormatting(format);
        factory.setContentType(format, [](const std::optional<FormatSettings> & settings)
        {
            return settings && settings->json.array_of_rows ? "application/json; charset=UTF-8" : "application/x-ndjson; charset=UTF-8";
        });
        /// The field names are emitted as JSON object keys every row via `makeNamesValidJSONStrings`
        /// with `output_format_json_validate_utf8`. When validation is off, a name that is not valid
        /// UTF-8 (a quoted alias with arbitrary bytes) makes the keys, and hence the output, non-textual.
        /// The row values can synthesize further object keys from named `Tuple` element names (see
        /// `tupleElementNamesMayProduceRawBytesInJSON`) - except when the values are serialized as
        /// strings (`JSONStringsEachRow`), where a `Tuple` value is written in its plain text form,
        /// which carries no element names - but that plain text form writes the `Bool`
        /// representations verbatim (see `boolRepresentationsMayProduceRawBytesInJSONStrings`).
        /// The `WithNames` variants write a header row of names (and the `WithNamesAndTypes` ones a
        /// row of type names too) via `makeNamesValidJSONStrings` with `output_format_json_validate_utf8`.
        /// All of this is knowable from the header and the settings, so the text framings reject or
        /// base64-encode accordingly.
        factory.registerOutputFormatMayProduceRawBytesChecker(
            format,
            [with_types, serialize_as_strings](const FormatSettings & settings, const Block & header)
            {
                /// The column names are written by every variant (in the data rows, and additionally in
                /// the header row of the `WithNames*` ones), the type names only by `WithNamesAndTypes`.
                return JSONUtils::namesMayProduceRawBytesInJSON(header.getNames(), settings, settings.json.validate_utf8)
                    || (with_types
                        && JSONUtils::namesMayProduceRawBytesInJSON(header.getDataTypeNames(), settings, settings.json.validate_utf8))
                    || (!serialize_as_strings
                        && JSONUtils::tupleElementNamesMayProduceRawBytesInJSON(header, settings, settings.json.validate_utf8))
                    || (serialize_as_strings
                        && JSONUtils::boolRepresentationsMayProduceRawBytesInJSONStrings(header, settings, settings.json.validate_utf8));
            });
    };

    /// JSONEachRow family (typed JSON values)
    register_function("JSONEachRow", false, false, false, false);
    register_function("JSONEachRowWithNames", false, false, true, false);
    register_function("JSONEachRowWithNamesAndTypes", false, false, true, true);

    /// JSONStringsEachRow family (all values as JSON strings)
    register_function("JSONStringsEachRow", true, false, false, false);
    register_function("JSONStringsEachRowWithNames", true, false, true, false);
    register_function("JSONStringsEachRowWithNamesAndTypes", true, false, true, true);

    register_function("PrettyJSONEachRow", false, true, false, false);
    register_function("JSONLines", false, false, false, false);
    register_function("PrettyJSONLines", false, true, false, false);
    register_function("NDJSON", false, false, false, false);
    register_function("PrettyNDJSON", false, true, false, false);
    register_function("JSONL", false, false, false, false);

    /// `registerOutputFormat` auto-registers a file extension equal to the (lower-cased) format name,
    /// so registering `JSONL` for output above re-points the `jsonl` extension to the `JSONL` format,
    /// undoing the explicit `jsonl` -> `JSONEachRow` mapping from `registerInputFormatJSONEachRow`.
    /// Re-assert it here (this function runs after the input one) so that files with a `.jsonl`
    /// extension are still resolved to the canonical `JSONEachRow` format.
    factory.registerFileExtension("jsonl", "JSONEachRow");

    factory.setDocumentation("PrettyJSONEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                             |
|-------|--------|-----------------------------------|
| ✗     | ✔      | `PrettyJSONLines`, `PrettyNDJSON` |

## Description {#description}

Differs from [JSONEachRow](/reference/formats/JSON/JSONEachRow) only in that JSON is pretty formatted with new line delimiters and 4 space indents.

## Example usage {#example-usage}
### Reading data {#reading-data}

Read data using the `PrettyJSONEachRow` format:

```sql
SELECT *
FROM football
FORMAT PrettyJSONEachRow
```

The output will be in JSON format:

```json
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Sutton United",
    "away_team": "Bradford City",
    "home_team_goals": 1,
    "away_team_goals": 4
}
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Swindon Town",
    "away_team": "Barrow",
    "home_team_goals": 2,
    "away_team_goals": 1
}
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Tranmere Rovers",
    "away_team": "Oldham Athletic",
    "home_team_goals": 2,
    "away_team_goals": 0
}
{
    "date": "2022-05-02",
    "season": 2021,
    "home_team": "Port Vale",
    "away_team": "Newport County",
    "home_team_goals": 1,
    "away_team_goals": 2
}
{
    "date": "2022-05-02",
    "season": 2021,
    "home_team": "Salford City",
    "away_team": "Mansfield Town",
    "home_team_goals": 2,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Barrow",
    "away_team": "Northampton Town",
    "home_team_goals": 1,
    "away_team_goals": 3
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Bradford City",
    "away_team": "Carlisle United",
    "home_team_goals": 2,
    "away_team_goals": 0
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Bristol Rovers",
    "away_team": "Scunthorpe United",
    "home_team_goals": 7,
    "away_team_goals": 0
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Exeter City",
    "away_team": "Port Vale",
    "home_team_goals": 0,
    "away_team_goals": 1
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Harrogate Town A.F.C.",
    "away_team": "Sutton United",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Hartlepool United",
    "away_team": "Colchester United",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Leyton Orient",
    "away_team": "Tranmere Rovers",
    "home_team_goals": 0,
    "away_team_goals": 1
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Mansfield Town",
    "away_team": "Forest Green Rovers",
    "home_team_goals": 2,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Newport County",
    "away_team": "Rochdale",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Oldham Athletic",
    "away_team": "Crawley Town",
    "home_team_goals": 3,
    "away_team_goals": 3
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Stevenage Borough",
    "away_team": "Salford City",
    "home_team_goals": 4,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Walsall",
    "away_team": "Swindon Town",
    "home_team_goals": 0,
    "away_team_goals": 3
}  
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("PrettyJSONLines", Documentation{
        .description = "An alias for the `PrettyJSONEachRow` format. See the `PrettyJSONEachRow` entry for the full documentation.",
        .related = {"PrettyJSONEachRow"}});

    factory.setDocumentation("PrettyNDJSON", Documentation{
        .description = "An alias for the `PrettyJSONEachRow` format. See the `PrettyJSONEachRow` entry for the full documentation.",
        .related = {"PrettyJSONEachRow"}});

    factory.setDocumentation("JSONEachRowWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONEachRow`](/reference/formats/JSON/JSONEachRow) format in that it also prints the header row with column names, similar to the [`TabSeparatedWithNames`](/reference/formats/TabSeparated/TabSeparatedWithNames) format.
Data rows are still output as JSON objects, not arrays.

## Example usage {#example-usage}

### Reading data {#reading-data}

Read data using the `JSONEachRowWithNames` format:

```sql
SELECT *
FROM football
FORMAT JSONEachRowWithNames
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
```

## Format settings {#format-settings}

<Note>
The data rows are JSON objects, so on input the columns are always matched by the names inside each
row, as in the `JSONEachRow` format: the header row of names is read and skipped. Columns with unknown
names are skipped if setting [`input_format_skip_unknown_fields`](/reference/settings/formats/input-format#input_format_skip_unknown_fields) is set to 1.
</Note>
)DOCS_MD"});

    factory.setDocumentation("JSONEachRowWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONEachRow`](/reference/formats/JSON/JSONEachRow) format in that it also prints two header rows with column names and types, similar to the [TabSeparatedWithNamesAndTypes](/reference/formats/TabSeparated/TabSeparatedWithNamesAndTypes) format.
Data rows are still output as JSON objects, not arrays.

## Example usage {#example-usage}

### Reading data {#reading-data}

Read data using the `JSONEachRowWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT JSONEachRowWithNamesAndTypes
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
```

## Format settings {#format-settings}

<Note>
The data rows are JSON objects, so on input the columns are always matched by the names inside each
row, as in the `JSONEachRow` format: the header row of names is read and skipped. Columns with unknown
names are skipped if setting [`input_format_skip_unknown_fields`](/reference/settings/formats/input-format#input_format_skip_unknown_fields) is set to 1.
</Note>

<Note>
If setting [`input_format_with_types_use_header`](/reference/settings/formats/input-format#input_format_with_types_use_header) is set to 1,
the types in the header row are checked against the types of the destination columns and a mismatch is an error.
</Note>
)DOCS_MD"});

    factory.setDocumentation("JSONStringsEachRowWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONStringsEachRow`](/reference/formats/JSON/JSONStringsEachRow) format in that it also prints the header row with column names, similar to the [`TabSeparatedWithNames`](/reference/formats/TabSeparated/TabSeparatedWithNames) format.
Data rows are still output as JSON objects, not arrays.

## Example usage {#example-usage}

### Reading data {#reading-data}

Read data using the `JSONStringsEachRowWithNames` format:

```sql
SELECT *
FROM football
FORMAT JSONStringsEachRowWithNames
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
{"date":"2022-04-30","season":"2021","home_team":"Sutton United","away_team":"Bradford City","home_team_goals":"1","away_team_goals":"4"}
{"date":"2022-04-30","season":"2021","home_team":"Swindon Town","away_team":"Barrow","home_team_goals":"2","away_team_goals":"1"}
```

## Format settings {#format-settings}

<Note>
The data rows are JSON objects, so on input the columns are always matched by the names inside each
row, as in the `JSONEachRow` format: the header row of names is read and skipped. Columns with unknown
names are skipped if setting [`input_format_skip_unknown_fields`](/reference/settings/formats/input-format#input_format_skip_unknown_fields) is set to 1.
</Note>
)DOCS_MD"});

    factory.setDocumentation("JSONStringsEachRowWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONStringsEachRow`](/reference/formats/JSON/JSONStringsEachRow) format in that it also prints two header rows with column names and types, similar to the [TabSeparatedWithNamesAndTypes](/reference/formats/TabSeparated/TabSeparatedWithNamesAndTypes) format.
Data rows are still output as JSON objects, not arrays.

## Example usage {#example-usage}

### Reading data {#reading-data}

Read data using the `JSONStringsEachRowWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT JSONStringsEachRowWithNamesAndTypes
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
{"date":"2022-04-30","season":"2021","home_team":"Sutton United","away_team":"Bradford City","home_team_goals":"1","away_team_goals":"4"}
{"date":"2022-04-30","season":"2021","home_team":"Swindon Town","away_team":"Barrow","home_team_goals":"2","away_team_goals":"1"}
```

## Format settings {#format-settings}

<Note>
The data rows are JSON objects, so on input the columns are always matched by the names inside each
row, as in the `JSONEachRow` format: the header row of names is read and skipped. Columns with unknown
names are skipped if setting [`input_format_skip_unknown_fields`](/reference/settings/formats/input-format#input_format_skip_unknown_fields) is set to 1.
</Note>

<Note>
If setting [`input_format_with_types_use_header`](/reference/settings/formats/input-format#input_format_with_types_use_header) is set to 1,
the types in the header row are checked against the types of the destination columns and a mismatch is an error.
</Note>
)DOCS_MD"});
}

}
