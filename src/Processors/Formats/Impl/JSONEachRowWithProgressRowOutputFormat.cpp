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

Differs from [`JSONEachRow`](./JSONEachRow.md)/[`JSONStringsEachRow`](./JSONStringsEachRow.md) in that ClickHouse will also yield progress information as JSON values.

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

Differs from `JSONEachRow`/`JSONStringsEachRow` in that ClickHouse will also yield progress information as JSON values.

## Example usage {#example-usage}

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## Format settings {#format-settings}
)DOCS_MD"});
}

}
