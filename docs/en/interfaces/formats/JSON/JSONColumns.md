---
alias: []
description: 'Documentation for the JSONColumns format'
input_format: true
keywords: ['JSONColumns']
output_format: true
slug: /interfaces/formats/JSONColumns
title: 'JSONColumns'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

:::tip
The output of the JSONColumns* formats provides the ClickHouse field name and then the content of each row in the table for that field;
visually, the data is rotated 90 degrees to the left.
:::

In this format, all data is represented as a single JSON Object.

:::note
The `JSONColumns` format buffers all data in memory and then outputs it as a single block, so, it can lead to high memory consumption.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONColumns;
```

### Reading data {#reading-data}

Read data using the `JSONColumns` format:

```sql
SELECT *
FROM football
FORMAT JSONColumns
```

The output will be in JSON format:

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

## Format settings {#format-settings}

During import, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Columns that are not present in the block will be filled with default values (you can use the [`input_format_defaults_for_omitted_fields`](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting here)