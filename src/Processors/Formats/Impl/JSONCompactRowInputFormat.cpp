#include <Processors/Formats/Impl/JSONCompactRowInputFormat.h>

#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

JSONCompactRowInputFormat::JSONCompactRowInputFormat(
    SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes<JSONCompactFormatReader>(
        header_, in_, params_, false, false, false, format_settings_, std::make_unique<JSONCompactFormatReader>(in_, format_settings_), false, false)
{
}

void JSONCompactRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
    JSONUtils::skipObjectStart(*in);
    if (format_settings.json.validate_types_from_metadata)
    {
        auto names_and_types = JSONUtils::readMetadataAndValidateHeader(*in, getPort().getHeader(), format_settings.json);
        Names column_names;
        for (const auto & [name, type] : names_and_types)
            column_names.push_back(name);
        column_mapping->addColumns(column_names, column_indexes_by_names, format_settings);
    }
    else
    {
        JSONUtils::readMetadata(*in, format_settings.json);
        column_mapping->setupByHeader(getPort().getHeader());
    }

    JSONUtils::skipComma(*in);
    if (!JSONUtils::skipUntilFieldInObject(*in, "data", format_settings.json))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"data\" with table content");

    JSONUtils::skipArrayStart(*in);
}

void JSONCompactRowInputFormat::readSuffix()
{
    /// Array end was skipped in JSONCompactFormatReader::checkForSuffix
    JSONUtils::skipTheRestOfObject(*in, format_settings.json);
}

void JSONCompactRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

JSONCompactFormatReader::JSONCompactFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : JSONCompactEachRowFormatReader(in_, false, format_settings_)
{
}

bool JSONCompactFormatReader::checkForSuffix()
{
    return JSONUtils::checkAndSkipArrayEnd(*in);
}

void registerInputFormatJSONCompact(FormatFactory & factory);
void registerInputFormatJSONCompact(FormatFactory & factory)
{
    factory.registerInputFormat("JSONCompact", [](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
    {
        return std::make_shared<JSONCompactRowInputFormat>(std::make_unique<const Block>(sample), buf, std::move(params), settings);
    });

    factory.markFormatSupportsSubsetOfColumns("JSONCompact");

    factory.setDocumentation("JSONCompact", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from [JSON](./JSON.md) only in that data rows are output as arrays, not as objects.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

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
        ["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4],
        ["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1],
        ["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0],
        ["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2],
        ["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2],
        ["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3],
        ["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0],
        ["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0],
        ["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1],
        ["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2],
        ["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2],
        ["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1],
        ["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2],
        ["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2],
        ["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3],
        ["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2],
        ["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
    ]
}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompact;
```

### Reading data {#reading-data}

Read data using the `JSONCompact` format:

```sql
SELECT *
FROM football
FORMAT JSONCompact
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
        ["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4],
        ["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1],
        ["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0],
        ["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2],
        ["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2],
        ["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3],
        ["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0],
        ["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0],
        ["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1],
        ["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2],
        ["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2],
        ["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1],
        ["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2],
        ["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2],
        ["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3],
        ["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2],
        ["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
    ],

    "rows": 17,

    "statistics":
    {
        "elapsed": 0.223690876,
        "rows_read": 0,
        "bytes_read": 0
    }
}
```

## Format settings {#format-settings}
)DOCS_MD"});
}

}
