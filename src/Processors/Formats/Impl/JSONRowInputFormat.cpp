#include <Processors/Formats/Impl/JSONRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

JSONRowInputFormat::JSONRowInputFormat(ReadBuffer & in_, SharedHeader header_, Params params_, const FormatSettings & format_settings_)
    : JSONRowInputFormat(std::make_unique<PeekableReadBuffer>(in_), header_, params_, format_settings_)
{
}

JSONRowInputFormat::JSONRowInputFormat(std::unique_ptr<PeekableReadBuffer> buf, SharedHeader header_, DB::IRowInputFormat::Params params_, const DB::FormatSettings & format_settings_)
    : JSONEachRowRowInputFormat(*buf, header_, params_, format_settings_, false), validate_types_from_metadata(format_settings_.json.validate_types_from_metadata), peekable_buf(std::move(buf))
{
}

void JSONRowInputFormat::readPrefix()
{
    skipBOMIfExists(*peekable_buf);

    PeekableReadBufferCheckpoint checkpoint(*peekable_buf);
    NamesAndTypesList names_and_types_from_metadata;

    /// Try to parse metadata, if failed, try to parse data as JSONEachRow format.
    if (JSONUtils::checkAndSkipObjectStart(*peekable_buf)
        && JSONUtils::tryReadMetadata(*peekable_buf, names_and_types_from_metadata, format_settings.json)
        && JSONUtils::checkAndSkipComma(*peekable_buf)
        && JSONUtils::skipUntilFieldInObject(*peekable_buf, "data", format_settings.json)
        && JSONUtils::checkAndSkipArrayStart(*peekable_buf))
    {
        data_in_square_brackets = true;
        if (validate_types_from_metadata)
        {
            JSONUtils::validateMetadataByHeader(names_and_types_from_metadata, getPort().getHeader());
        }
    }
    else
    {
        parse_as_json_each_row = true;
        peekable_buf->rollbackToCheckpoint();
        JSONEachRowRowInputFormat::readPrefix();
    }
}

void JSONRowInputFormat::readSuffix()
{
    if (parse_as_json_each_row)
    {
        JSONEachRowRowInputFormat::readSuffix();
    }
    else
    {
        JSONUtils::skipArrayEnd(*peekable_buf);
        JSONUtils::skipTheRestOfObject(*peekable_buf, format_settings.json);
    }
}

void JSONRowInputFormat::setReadBuffer(DB::ReadBuffer & in_)
{
    peekable_buf = std::make_unique<PeekableReadBuffer>(in_);
    JSONEachRowRowInputFormat::setReadBuffer(*peekable_buf);
}

void JSONRowInputFormat::resetReadBuffer()
{
    peekable_buf.reset();
    JSONEachRowRowInputFormat::resetReadBuffer();
}

JSONRowSchemaReader::JSONRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, bool fallback_to_json_each_row_)
    : JSONRowSchemaReader(std::make_unique<PeekableReadBuffer>(in_), format_settings_, fallback_to_json_each_row_)
{
}

JSONRowSchemaReader::JSONRowSchemaReader(std::unique_ptr<PeekableReadBuffer> buf, const DB::FormatSettings & format_settings_, bool fallback_to_json_each_row_)
    : JSONEachRowSchemaReader(*buf, format_settings_), peekable_buf(std::move(buf)), fallback_to_json_each_row(fallback_to_json_each_row_)
{
}

NamesAndTypesList JSONRowSchemaReader::readSchema()
{
    skipBOMIfExists(*peekable_buf);

    if (fallback_to_json_each_row)
    {
        PeekableReadBufferCheckpoint checkpoint(*peekable_buf);
        /// Try to parse metadata, if failed, try to parse data as JSONEachRow format
        NamesAndTypesList names_and_types;
        if (JSONUtils::checkAndSkipObjectStart(*peekable_buf) && JSONUtils::tryReadMetadata(*peekable_buf, names_and_types, format_settings.json))
            return names_and_types;

        peekable_buf->rollbackToCheckpoint(true);
        return JSONEachRowSchemaReader::readSchema();
    }

    JSONUtils::skipObjectStart(*peekable_buf);
    return JSONUtils::readMetadata(*peekable_buf, format_settings.json);
}

void registerInputFormatJSON(FormatFactory & factory);
void registerInputFormatJSON(FormatFactory & factory)
{
    factory.registerInputFormat("JSON", [](
                     ReadBuffer & buf,
                     const Block & sample,
                     IRowInputFormat::Params params,
                     const FormatSettings & settings)
    {
        return std::make_shared<JSONRowInputFormat>(buf, std::make_shared<const Block>(sample), std::move(params), settings);
    });

    factory.markFormatSupportsSubsetOfColumns("JSON");

    factory.setDocumentation("JSON", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `JSON` format reads and outputs data in the JSON format. 

The `JSON` format returns the following: 

| Parameter                    | Description                                                                                                                                                                                                                                |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `meta`                       | Column names and types.                                                                                                                                                                                                                    |
| `data`                       | Data tables                                                                                                                                                                                                                                |
| `rows`                       | The total number of output rows.                                                                                                                                                                                                           |
| `rows_before_limit_at_least` | The lower estimate of the number of rows there would have been without LIMIT. Output only if the query contains LIMIT. This estimate is calculated from the blocks of data processed in the query pipeline before the limit transform, but could then be discarded by the limit transform. If the blocks didn't even reach the limit transform in the query pipeline, they don't participate in the estimation. |
| `statistics`                 | Statistics such as `elapsed`, `rows_read`, `bytes_read`.                                                                                                                                                                                   |
| `totals`                     | Total values (when using WITH TOTALS).                                                                                                                                                                                                     |
| `extremes`                   | Extreme values (when extremes are set to 1).                                                                                                                                                                                               |

The `JSON` type is compatible with JavaScript. To ensure this, some characters are additionally escaped: 
- the slash `/` is escaped as `\/`
- alternative line breaks `U+2028` and `U+2029`, which break some browsers, are escaped as `\uXXXX`. 
- ASCII control characters are escaped: backspace, form feed, line feed, carriage return, and horizontal tab are replaced with `\b`, `\f`, `\n`, `\r`, `\t` , as well as the remaining bytes in the 00-1F range using `\uXXXX` sequences. 
- Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. 

For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double quotes by default. 
To remove the quotes, you can set the configuration parameter [`output_format_json_quote_64bit_integers`](/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) to `0`.

ClickHouse supports [NULL](/sql-reference/syntax.md), which is displayed as `null` in the JSON output. To enable `+nan`, `-nan`, `+inf`, `-inf` values in output, set the [output_format_json_quote_denormals](/operations/settings/settings-formats.md/#output_format_json_quote_denormals) to `1`.

## Example usage {#example-usage}

Example:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

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
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## Format settings {#format-settings}

For JSON input format, if setting [`input_format_json_validate_types_from_metadata`](/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) is set to `1`,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

## See also {#see-also}

- [JSONEachRow](/interfaces/formats/JSONEachRow) format
- [output_format_json_array_of_rows](/operations/settings/settings-formats.md/#output_format_json_array_of_rows) setting
)DOCS_MD"});
}

void registerJSONSchemaReader(FormatFactory & factory);
void registerJSONSchemaReader(FormatFactory & factory)
{
    auto register_schema_reader = [&](const String & format, bool fallback_to_json_each_row)
    {
        factory.registerSchemaReader(
            format, [fallback_to_json_each_row](ReadBuffer & buf, const FormatSettings & format_settings) { return std::make_unique<JSONRowSchemaReader>(buf, format_settings, fallback_to_json_each_row); });

        factory.registerAdditionalInfoForSchemaCacheGetter(format, [](const FormatSettings & settings)
        {
            return getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
        });
    };
    register_schema_reader("JSON", true);
    /// JSONCompact has the same suffix with metadata.
    register_schema_reader("JSONCompact", false);
}

}
