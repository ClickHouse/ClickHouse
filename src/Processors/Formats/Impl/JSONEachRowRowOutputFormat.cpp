#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>


namespace DB
{


JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat(
    WriteBuffer & out_,
    SharedHeader header_,
    const FormatSettings & settings_,
    bool pretty_json_)
    : RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>(
        header_, out_, settings_.json.valid_output_on_exception, settings_.json.validate_utf8)
    , pretty_json(pretty_json_)
    , settings(settings_)
{
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
    auto register_function = [&](const String & format, bool serialize_as_strings, bool pretty_json)
    {
        factory.registerOutputFormat(format, [serialize_as_strings, pretty_json](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings,
            FormatFilterInfoPtr /*format_filter_info*/)
        {
            FormatSettings settings = _format_settings;
            settings.json.serialize_as_strings = serialize_as_strings;
            return std::make_shared<JSONEachRowRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings, pretty_json);
        });
        factory.markOutputFormatSupportsParallelFormatting(format);
        factory.setContentType(format, [](const std::optional<FormatSettings> & settings)
        {
            return settings && settings->json.array_of_rows ? "application/json; charset=UTF-8" : "application/x-ndjson; charset=UTF-8";
        });
    };

    register_function("JSONEachRow", false, false);
    register_function("PrettyJSONEachRow", false, true);
    register_function("JSONLines", false, false);
    register_function("PrettyJSONLines", false, true);
    register_function("NDJSON", false, false);
    register_function("PrettyNDJSON", false, true);
    register_function("JSONL", false, false);
    register_function("JSONStringsEachRow", true, false);

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

Differs from [JSONEachRow](./JSONEachRow.md) only in that JSON is pretty formatted with new line delimiters and 4 space indents.

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
}

}
