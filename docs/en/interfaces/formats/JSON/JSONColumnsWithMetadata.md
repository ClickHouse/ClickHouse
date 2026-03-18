---
alias: []
description: 'Documentation for the JSONColumnsWithMetadata format'
input_format: true
keywords: ['JSONColumnsWithMetadata']
output_format: true
slug: /interfaces/formats/JSONColumnsWithMetadata
title: 'JSONColumnsWithMetadata'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONColumns`](./JSONColumns.md) format in that it also contains some metadata and statistics (similar to the [`JSON`](./JSON.md) format).

:::note
The `JSONColumnsWithMetadata` format buffers all data in memory and then outputs it as a single block, so, it can lead to high memory consumption.
:::

## Example Usage {#example-usage}

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

## Format Settings {#format-settings}