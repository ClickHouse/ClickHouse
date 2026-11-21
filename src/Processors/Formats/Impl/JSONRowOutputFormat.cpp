#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>


namespace DB
{

JSONRowOutputFormat::JSONRowOutputFormat(
    WriteBuffer & out_,
    SharedHeader header,
    const FormatSettings & settings_,
    bool yield_strings_)
    : RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>(header, out_, settings_.json.valid_output_on_exception, true), settings(settings_), yield_strings(yield_strings_)
{
    names = JSONUtils::makeNamesValidJSONStrings(header->getNames(), settings, true);
    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
    settings.json.pretty_print_indent = '\t';
    settings.json.pretty_print_indent_multiplier = 1;
}


void JSONRowOutputFormat::writePrefix()
{
    JSONUtils::writeObjectStart(*ostr);
    JSONUtils::writeMetadata(names, types, settings, *ostr);
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeArrayStart(*ostr, 1, "data");
}


void JSONRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    JSONUtils::writeFieldFromColumn(column, serialization, row_num, yield_strings, settings, *ostr, names[field_number], 3, " ", settings.json.pretty_print);
    ++field_number;
}

void JSONRowOutputFormat::writeFieldDelimiter()
{
    JSONUtils::writeFieldDelimiter(*ostr);
}


void JSONRowOutputFormat::writeRowStartDelimiter()
{
    JSONUtils::writeObjectStart(*ostr, 2);
}


void JSONRowOutputFormat::writeRowEndDelimiter()
{
    JSONUtils::writeObjectEnd(*ostr, 2);
    field_number = 0;
    ++row_count;
}


void JSONRowOutputFormat::writeRowBetweenDelimiter()
{
    JSONUtils::writeFieldDelimiter(*ostr);
}


void JSONRowOutputFormat::writeSuffix()
{
    JSONUtils::writeArrayEnd(*ostr, 1);
}

void JSONRowOutputFormat::writeBeforeTotals()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "totals");
}

void JSONRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    JSONUtils::writeColumns(columns, names, serializations, row_num, yield_strings, settings, *ostr, 2);
}

void JSONRowOutputFormat::writeAfterTotals()
{
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONRowOutputFormat::writeBeforeExtremes()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "extremes");
}

void JSONRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    JSONUtils::writeObjectStart(*ostr, 2, title);
    JSONUtils::writeColumns(columns, names, serializations, row_num, yield_strings, settings, *ostr, 3);
    JSONUtils::writeObjectEnd(*ostr, 2);
}

void JSONRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("min", columns, row_num);
}

void JSONRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("max", columns, row_num);
}

void JSONRowOutputFormat::writeAfterExtremes()
{
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONRowOutputFormat::finalizeImpl()
{
    JSONUtils::writeAdditionalInfo(
        row_count,
        statistics.rows_before_limit,
        statistics.applied_limit,
        statistics.rows_before_aggregation,
        statistics.applied_aggregation,
        statistics.watch,
        statistics.progress,
        settings.write_statistics && exception_message.empty(),
        *ostr);

    if (!exception_message.empty())
    {
        writeCString(",\n\n", *ostr);
        JSONUtils::writeException(exception_message, *ostr, settings, 1);
    }

    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
    ostr->next();
}

void JSONRowOutputFormat::resetFormatterImpl()
{
    RowOutputFormatWithExceptionHandlerAdaptor::resetFormatterImpl();
    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
    row_count = 0;
    statistics = Statistics();
}

void registerOutputFormatJSON(FormatFactory & factory);
void registerOutputFormatJSON(FormatFactory & factory)
{
    factory.registerOutputFormat("JSON", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings, false);
    });

    factory.markOutputFormatSupportsParallelFormatting("JSON");
    factory.markFormatHasNoAppendSupport("JSON");
    factory.setContentType("JSON", "application/json; charset=UTF-8");

    factory.registerOutputFormat("JSONStrings", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings, true);
    });

    factory.markOutputFormatSupportsParallelFormatting("JSONStrings");
    factory.markFormatHasNoAppendSupport("JSONStrings");
    factory.setContentType("JSONStrings", "application/json; charset=UTF-8");

    factory.setDocumentation("JSONStrings", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Differs from the [JSON](./JSON.md) format only in that data fields are output as strings, not as typed JSON values. This is an output-only format.

## Example usage {#example-usage}

### Reading data {#reading-data}

Read data using the `JSONStrings` format:

```sql
SELECT *
FROM football
FORMAT JSONStrings
```

The output will be in JSON format:

```json
{
    "meta":
    [
            {
                    "name": "date",
                    "type": "Date"
            },
            {
                    "name": "season",
                    "type": "Int16"
            },
            {
                    "name": "home_team",
                    "type": "LowCardinality(String)"
            },
            {
                    "name": "away_team",
                    "type": "LowCardinality(String)"
            },
            {
                    "name": "home_team_goals",
                    "type": "Int8"
            },
            {
                    "name": "away_team_goals",
                    "type": "Int8"
            }
    ],

    "data":
    [
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Sutton United",
                    "away_team": "Bradford City",
                    "home_team_goals": "1",
                    "away_team_goals": "4"
            },
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Swindon Town",
                    "away_team": "Barrow",
                    "home_team_goals": "2",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Tranmere Rovers",
                    "away_team": "Oldham Athletic",
                    "home_team_goals": "2",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-02",
                    "season": "2021",
                    "home_team": "Port Vale",
                    "away_team": "Newport County",
                    "home_team_goals": "1",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-02",
                    "season": "2021",
                    "home_team": "Salford City",
                    "away_team": "Mansfield Town",
                    "home_team_goals": "2",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Barrow",
                    "away_team": "Northampton Town",
                    "home_team_goals": "1",
                    "away_team_goals": "3"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Bradford City",
                    "away_team": "Carlisle United",
                    "home_team_goals": "2",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Bristol Rovers",
                    "away_team": "Scunthorpe United",
                    "home_team_goals": "7",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Exeter City",
                    "away_team": "Port Vale",
                    "home_team_goals": "0",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Harrogate Town A.F.C.",
                    "away_team": "Sutton United",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Hartlepool United",
                    "away_team": "Colchester United",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Leyton Orient",
                    "away_team": "Tranmere Rovers",
                    "home_team_goals": "0",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Mansfield Town",
                    "away_team": "Forest Green Rovers",
                    "home_team_goals": "2",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Newport County",
                    "away_team": "Rochdale",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Oldham Athletic",
                    "away_team": "Crawley Town",
                    "home_team_goals": "3",
                    "away_team_goals": "3"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Stevenage Borough",
                    "away_team": "Salford City",
                    "home_team_goals": "4",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Walsall",
                    "away_team": "Swindon Town",
                    "home_team_goals": "0",
                    "away_team_goals": "3"
            }
    ],

    "rows": 17,

    "statistics":
    {
            "elapsed": 0.173464376,
            "rows_read": 0,
            "bytes_read": 0
    }
}
```

## Format settings {#format-settings}
)DOCS_MD"});
}

}
