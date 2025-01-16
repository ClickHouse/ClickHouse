---
title : JSONColumnsWithMetadata
slug : /en/interfaces/formats/JSONColumnsWithMetadata
keywords : [JSONColumnsWithMetadata]
---

## Description

Differs from JSONColumns format in that it also contains some metadata and statistics (similar to JSON format).
Output format buffers all data in memory and then outputs them as a single block, so, it can lead to high memory consumption.

## Example Usage

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

For JSONColumnsWithMetadata input format, if setting [input_format_json_validate_types_from_metadata](/docs/en/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) is set to 1,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

## Format Settings