#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>


namespace DB
{

void JSONEachRowWithProgressRowOutputFormat::writePrefix()
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

void JSONEachRowWithProgressRowOutputFormat::writeSuffix()
{
    /// Do not write exception here like JSONEachRow does. See finalizeImpl.
}

void JSONEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("{\"row\":{", *ostr);
}

void JSONEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("}}\n", *ostr);
    field_number = 0;
}

void JSONEachRowWithProgressRowOutputFormat::writeSpecialRow(const char * kind, const Columns & columns, size_t row_num)
{
    writeCString("{\"", *ostr);
    writeCString(kind, *ostr);
    writeCString("\":{", *ostr);

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }

    writeCString("}}\n", *ostr);
    field_number = 0;
}

void JSONEachRowWithProgressRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeSpecialRow("totals", columns, row_num);
}

void JSONEachRowWithProgressRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow("min", columns, row_num);
}

void JSONEachRowWithProgressRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow("max", columns, row_num);
}

void JSONEachRowWithProgressRowOutputFormat::writeProgress(const Progress & value)
{
    if (value.empty())
        return;
    writeCString("{\"progress\":", *ostr);
    value.writeJSON(*ostr, Progress::DisplayMode::Minimal);
    writeCString("}\n", *ostr);
}

void JSONEachRowWithProgressRowOutputFormat::finalizeImpl()
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

void registerOutputFormatJSONEachRowWithProgress(FormatFactory & factory);
void registerOutputFormatJSONEachRowWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings,
            FormatFilterInfoPtr /*format_filter_info*/)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });
    factory.setContentType("JSONEachRowWithProgress", "application/json; charset=UTF-8");

    factory.registerOutputFormat("JSONStringsEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings,
            FormatFilterInfoPtr /*format_filter_info*/)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = true;
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });
    factory.setContentType("JSONStringsEachRowWithProgress", "application/json; charset=UTF-8");

    factory.setDocumentation("JSONEachRowWithProgress", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Differs from [`JSONEachRow`](/reference/formats/JSON/JSONEachRow)/[`JSONStringsEachRow`](/reference/formats/JSON/JSONStringsEachRow) in that ClickHouse streams each event as a separate JSON object and also yields progress information.

The related `JSONStringsEachRowWithProgress` format uses the same top-level object kinds; field values are serialized as strings.

The compact siblings `JSONCompactEachRowWithProgress` and `JSONCompactStringsEachRowWithProgress` share the same stream-object kinds and progress/`LIMIT`/exception caveats; their `row` / `totals` / `min` / `max` payloads are value arrays instead of named objects.

## Stream objects {#stream-objects}

Each line of the response is one JSON object. Clients should dispatch on the top-level key:

| Top-level key | When it appears | Shape |
|---------------|-----------------|-------|
| `meta` | Once, before the first `row` (a `progress` object may be emitted before it) | `{"meta":[{"name":...,"type":...}, ...]}` — column names and types |
| `row` | Once per result row | `{"row":{...}}` — column values for that row |
| `progress` | Periodically while the query runs | `{"progress":{...}}` — counters such as `read_rows`, `read_bytes`, `total_rows_to_read` |
| `totals` | When totals are present | `{"totals":{...}}` — totals row values |
| `min` / `max` | When extremes are present | `{"min":{...}}` / `{"max":{...}}` — extreme row values |
| `rows_before_limit_at_least` | When the query contains `LIMIT` | `{"rows_before_limit_at_least":N}` — lower estimate of rows there would have been without `LIMIT` (not proof that rows were dropped) |
| `rows_before_aggregation` | When the query performs aggregation and the `rows_before_aggregation` counter is enabled | `{"rows_before_aggregation":N}` |
| `exception` | When the query fails, on the HTTP path with `http_write_exception_in_output_format=1` | `{"exception":"..."}` — error text as a **top-level string**, not nested under `row` |

The `rows_before_limit_at_least` object is emitted when the query contains `LIMIT`, even if the limit did not drop any rows. It is a lower estimate of the number of rows there would have been without `LIMIT` (same meaning as in `JSON`); clients must not treat it as proof that rows were dropped.

`meta` is emitted once before the first `row`, but a `progress` object can arrive before `meta`. The top-level `exception` object is emitted on the HTTP path only when `http_write_exception_in_output_format=1`; otherwise the error surfaces through the transport. When emitted it is a separate top-level object, not nested under `row`.

## Example usage {#example-usage}

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONStringsEachRowWithProgress", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Differs from `JSONEachRow`/`JSONStringsEachRow` in that ClickHouse streams each event as a separate JSON object and also yields progress information.

All field values are serialized as strings (including complex types): for example `arr` is emitted as `"arr":"[0,1]"`, not as a nested JSON array. Scalars are likewise stringified (`"num":"42"`).

The top-level object kinds match [`JSONEachRowWithProgress`](/reference/formats/JSON/JSONEachRowWithProgress) (`meta`, `row`, `progress`, `totals`/`min`/`max`, `rows_before_limit_at_least`, `rows_before_aggregation`, and top-level `exception`). Follow that page for the full stream contract: a `progress` object may arrive before `meta`; `rows_before_limit_at_least` is a lower estimate when the query contains `LIMIT` (not proof rows were dropped); `exception` is only written on the HTTP path with `http_write_exception_in_output_format=1`.

## Example usage {#example-usage}

```json
{"meta":[{"name":"num","type":"UInt8"},{"name":"str","type":"String"},{"name":"arr","type":"Array(UInt8)"}]}
{"row":{"num":"42","str":"hello","arr":"[0,1]"}}
{"row":{"num":"43","str":"hello","arr":"[0,1,2]"}}
{"row":{"num":"44","str":"hello","arr":"[0,1,2,3]"}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## Format settings {#format-settings}
)DOCS_MD"});
}

}
