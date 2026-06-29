#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONCompactEachRowWithProgressRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>


namespace DB
{

void JSONCompactEachRowWithProgressRowOutputFormat::writePrefix()
{
    writeCString("{\"meta\":[", *ostr);
    bool first = true;
    for (const auto & elem : getInputs().front().getHeader())
    {
        if (!first)
            writeChar(',', *ostr);
        first = false;
        writeCString("{\"name\":", *ostr);
        writeJSONString(elem.name, *ostr, settings);
        writeCString(",\"type\":", *ostr);
        writeJSONString(elem.type->getName(), *ostr, settings);
        writeChar('}', *ostr);
    }
    writeCString("]}\n", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeSuffix()
{
    /// Do not write exception here like JSONEachRow does. See finalizeImpl.
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("{\"row\":[", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("]}\n", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeSpecialRow(const char * kind, const Columns & columns, size_t row_num)
{
    writeCString("{\"", *ostr);
    writeCString(kind, *ostr);
    writeCString("\":[", *ostr);

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }

    writeCString("]}\n", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeSpecialRow("totals", columns, row_num);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow("min", columns, row_num);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow("max", columns, row_num);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeProgress(const Progress & value)
{
    if (value.empty())
        return;
    writeCString("{\"progress\":", *ostr);
    value.writeJSON(*ostr, Progress::DisplayMode::Minimal);
    writeCString("}\n", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::finalizeImpl()
{
    if (statistics.applied_limit)
    {
        writeCString("{\"rows_before_limit_at_least\":", *ostr);
        writeIntText(statistics.rows_before_limit, *ostr);
        writeCString("}\n", *ostr);
    }
    if (statistics.applied_aggregation)
    {
        writeCString("{\"rows_before_aggregation\":", *ostr);
        writeIntText(statistics.rows_before_aggregation, *ostr);
        writeCString("}\n", *ostr);
    }
    if (!exception_message.empty())
    {
        writeCString("{\"exception\":", *ostr);
        writeJSONString(exception_message, *ostr, settings);
        writeCString("}\n", *ostr);
    }
}

void registerOutputFormatJSONCompactEachRowWithProgress(FormatFactory & factory);
void registerOutputFormatJSONCompactEachRowWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONCompactEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings,
            FormatFilterInfoPtr /*format_filter_info*/)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONCompactEachRowWithProgressRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings, false, false);
    });

    factory.setContentType("JSONCompactEachRowWithProgress", "application/json; charset=UTF-8");

    factory.registerOutputFormat("JSONCompactStringsEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings,
            FormatFilterInfoPtr /*format_filter_info*/)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = true;
        return std::make_shared<JSONCompactEachRowWithProgressRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings, false, false);
    });

    factory.setContentType("JSONCompactStringsEachRowWithProgress", "application/json; charset=UTF-8");

    factory.setDocumentation("JSONCompactEachRowWithProgress", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

This format combines the compact row-by-row output of JSONCompactEachRow with streaming progress
information.
It outputs data as separate JSON objects for metadata, individual rows, progress updates,
totals, and exceptions. Values are represented in their native types.

Key features:
- Outputs metadata first with column names and types
- Each row is a separate JSON object with a "row" key containing an array of values
- Includes progress updates during query execution (as `{"progress":...}` objects)
- Supports totals and extremes
- Values keep their native types (numbers as numbers, strings as strings)

## Example usage {#example-usage}

```sql title="Query"
SELECT *
FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2)
LIMIT 5
FORMAT JSONCompactEachRowWithProgress
```

```response title="Response"
{"meta":[{"name":"a","type":"Array(Int8)"},{"name":"d","type":"Decimal(9, 4)"},{"name":"c","type":"Tuple(DateTime64(3), UUID)"}]}
{"row":[[-8], 46848.5225, ["2064-06-11 14:00:36.578","b06f4fa1-22ff-f84f-a1b7-a5807d983ae6"]]}
{"row":[[-76], -85331.598, ["2038-06-16 04:10:27.271","2bb0de60-3a2c-ffc0-d7a7-a5c88ed8177c"]]}
{"row":[[-32], -31470.8994, ["2027-07-18 16:58:34.654","1cdbae4c-ceb2-1337-b954-b175f5efbef8"]]}
{"row":[[-116], 32104.097, ["1979-04-27 21:51:53.321","66903704-3c83-8f8a-648a-da4ac1ffa9fc"]]}
{"row":[[], 2427.6614, ["1980-04-24 11:30:35.487","fee19be8-0f46-149b-ed98-43e7455ce2b2"]]}
{"progress":{"read_rows":"5","read_bytes":"184","total_rows_to_read":"5","elapsed_ns":"335771"}}
{"rows_before_limit_at_least":5}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStringsEachRowWithProgress", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias  |
|-------|---------|--------|
| ✗     | ✔       |        |

## Description {#description}

Similar to [`JSONCompactEachRowWithProgress`](/interfaces/formats/JSONCompactEachRowWithProgress), but all values are converted to strings.
This is useful when you need consistent string representation of all data types.

Key features:
- Same structure as `JSONCompactEachRowWithProgress`
- All values are represented as strings (numbers, arrays, etc. are all quoted strings)
- Includes progress updates, totals, and exception handling
- Useful for clients that prefer or require string-based data

## Example usage {#example-usage}

```sql title="Query"
SELECT *
FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2)
LIMIT 5
FORMAT JSONCompactStringsEachRowWithProgress
```

```response title="Response"
{"meta":[{"name":"a","type":"Array(Int8)"},{"name":"d","type":"Decimal(9, 4)"},{"name":"c","type":"Tuple(DateTime64(3), UUID)"}]}
{"row":["[-8]", "46848.5225", "('2064-06-11 14:00:36.578','b06f4fa1-22ff-f84f-a1b7-a5807d983ae6')"]}
{"row":["[-76]", "-85331.598", "('2038-06-16 04:10:27.271','2bb0de60-3a2c-ffc0-d7a7-a5c88ed8177c')"]}
{"row":["[-32]", "-31470.8994", "('2027-07-18 16:58:34.654','1cdbae4c-ceb2-1337-b954-b175f5efbef8')"]}
{"row":["[-116]", "32104.097", "('1979-04-27 21:51:53.321','66903704-3c83-8f8a-648a-da4ac1ffa9fc')"]}
{"row":["[]", "2427.6614", "('1980-04-24 11:30:35.487','fee19be8-0f46-149b-ed98-43e7455ce2b2')"]}
{"progress":{"read_rows":"5","read_bytes":"184","total_rows_to_read":"5","elapsed_ns":"191151"}}
{"rows_before_limit_at_least":5}
```

## Format settings {#format-settings}
)DOCS_MD"});
}

}
