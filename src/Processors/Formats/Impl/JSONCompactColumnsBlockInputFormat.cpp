#include <Processors/Formats/Impl/JSONCompactColumnsBlockInputFormat.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/JSONUtils.h>
#include <Processors/Port.h>

namespace DB
{

JSONCompactColumnsReader::JSONCompactColumnsReader(ReadBuffer & in_) : JSONColumnsReaderBase(in_)
{
}

void JSONCompactColumnsReader::readChunkStart()
{
    JSONUtils::skipArrayStart(*in);
}

std::optional<String> JSONCompactColumnsReader::readColumnStart()
{
    JSONUtils::skipArrayStart(*in);
    return std::nullopt;
}

bool JSONCompactColumnsReader::checkChunkEnd()
{
    return JSONUtils::checkAndSkipArrayEnd(*in);
}


void registerInputFormatJSONCompactColumns(FormatFactory & factory);
void registerInputFormatJSONCompactColumns(FormatFactory & factory)
{
    factory.registerInputFormat(
        "JSONCompactColumns",
        [](ReadBuffer & buf,
           const Block &sample,
           const RowInputFormatParams &,
           const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsBlockInputFormatBase>(buf, std::make_shared<const Block>(sample), settings, std::make_unique<JSONCompactColumnsReader>(buf));
        }
    );

    factory.setDocumentation("JSONCompactColumns", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

In this format, all data is represented as a single JSON Array.

:::note
The `JSONCompactColumns` output format buffers all data in memory to output it as a single block which can lead to high memory consumption.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
[
    ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactColumns;
```

### Reading data {#reading-data}

Read data using the `JSONCompactColumns` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactColumns
```

The output will be in JSON format:

```json
[
    ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
]
```

Columns that are not present in the block will be filled with default values (you can use [`input_format_defaults_for_omitted_fields`](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting here)

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerJSONCompactColumnsSchemaReader(FormatFactory & factory);
void registerJSONCompactColumnsSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "JSONCompactColumns",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsSchemaReaderBase>(buf, settings, std::make_unique<JSONCompactColumnsReader>(buf));
        }
    );
    factory.registerAdditionalInfoForSchemaCacheGetter("JSONCompactColumns", [](const FormatSettings & settings)
    {
        auto result = getAdditionalFormatInfoForAllRowBasedFormats(settings) + getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
        return result + fmt::format(", column_names_for_schema_inference={}", settings.column_names_for_schema_inference);
    });
}

}
