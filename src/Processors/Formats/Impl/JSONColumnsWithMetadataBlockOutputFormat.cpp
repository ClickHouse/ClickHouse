#include <Processors/Formats/Impl/JSONColumnsWithMetadataBlockOutputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JSONColumnsWithMetadataBlockOutputFormat::JSONColumnsWithMetadataBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : JSONColumnsBlockOutputFormat(out_, header_, format_settings_, true, 1), types(header_->getDataTypes())
{
}

void JSONColumnsWithMetadataBlockOutputFormat::writePrefix()
{
    JSONUtils::writeObjectStart(*ostr);
    JSONUtils::writeMetadata(names, types, format_settings, *ostr);
}

void JSONColumnsWithMetadataBlockOutputFormat::writeSuffix()
{
    rows = mono_chunk.getNumRows();
    JSONColumnsBlockOutputFormatBase::writeSuffix();
}

void JSONColumnsWithMetadataBlockOutputFormat::writeChunkStart()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "data");
}

void JSONColumnsWithMetadataBlockOutputFormat::writeChunkEnd()
{
    JSONUtils::writeObjectEnd(*ostr, indent);
}

void JSONColumnsWithMetadataBlockOutputFormat::consumeExtremes(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} in extremes chunk, expected 2", num_rows);

    const auto & columns = chunk.getColumns();
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "extremes");
    writeExtremesElement("min", columns, 0);
    JSONUtils::writeFieldDelimiter(*ostr);
    writeExtremesElement("max", columns, 1);
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONColumnsWithMetadataBlockOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    JSONUtils::writeObjectStart(*ostr, 2, title);
    JSONUtils::writeColumns(columns, names, serializations, row_num, false, format_settings, *ostr, 3);
    JSONUtils::writeObjectEnd(*ostr, 2);
}

void JSONColumnsWithMetadataBlockOutputFormat::consumeTotals(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} in totals chunk, expected 1", num_rows);

    const auto & columns = chunk.getColumns();
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "totals");
    JSONUtils::writeColumns(columns, names, serializations, 0, false, format_settings, *ostr, 2);
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONColumnsWithMetadataBlockOutputFormat::finalizeImpl()
{
    JSONUtils::writeAdditionalInfo(
        rows,
        statistics.rows_before_limit,
        statistics.applied_limit,
        statistics.rows_before_aggregation,
        statistics.applied_aggregation,
        statistics.watch,
        statistics.progress,
        format_settings.write_statistics,
        *ostr);

    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
    ostr->next();
}

void JSONColumnsWithMetadataBlockOutputFormat::resetFormatterImpl()
{
    JSONColumnsBlockOutputFormat::resetFormatterImpl();
    rows = 0;
    statistics = Statistics();
}

void registerOutputFormatJSONColumnsWithMetadata(FormatFactory & factory);
void registerOutputFormatJSONColumnsWithMetadata(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONColumnsWithMetadata", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<JSONColumnsWithMetadataBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings);
    });

    factory.markFormatHasNoAppendSupport("JSONColumnsWithMetadata");
    factory.setContentType("JSONColumnsWithMetadata", "application/json; charset=UTF-8");

    factory.setDocumentation("JSONColumnsWithMetadata", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONColumns`](./JSONColumns.md) format in that it also contains some metadata and statistics (similar to the [`JSON`](./JSON.md) format).

:::note
The `JSONColumnsWithMetadata` format buffers all data in memory and then outputs it as a single block, so, it can lead to high memory consumption.
:::

## Example usage {#example-usage}

Example:

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },

                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

For the `JSONColumnsWithMetadata` input format, if setting [`input_format_json_validate_types_from_metadata`](/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) is set to `1`,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

## Format settings {#format-settings}
)DOCS_MD"});
}

}
