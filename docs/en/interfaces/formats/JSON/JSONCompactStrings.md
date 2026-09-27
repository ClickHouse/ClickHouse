---
alias: []
description: 'Documentation for the JSONCompactStrings format'
input_format: false
keywords: ['JSONCompactStrings']
output_format: true
slug: /interfaces/formats/JSONCompactStrings
title: 'JSONCompactStrings'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `JSONCompactStrings` format differs from [JSONStrings](./JSONStrings.md) only in that data rows are output as arrays, not as objects.

## Example usage {#example-usage}

### Reading data {#reading-data}

Read data using the `JSONCompactStrings` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactStrings
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
            ["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"],
            ["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"],
            ["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"],
            ["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"],
            ["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"],
            ["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"],
            ["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"],
            ["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"],
            ["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"],
            ["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"],
            ["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"],
            ["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"],
            ["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"],
            ["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"],
            ["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"],
            ["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"],
            ["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
    ],

    "rows": 17,

    "statistics":
    {
            "elapsed": 0.112012501,
            "rows_read": 0,
            "bytes_read": 0
    }
}
```

## Format settings {#format-settings}