---
alias: []
description: 'Documentation for the JSONCompactStrings format'
input_format: false
keywords: ['JSONCompactStrings']
output_format: true
slug: /interfaces/formats/JSONCompactStrings
title: 'JSONCompactStrings'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `JSONCompactStrings` format differs from [JSONStrings](./JSONStrings.md) only in that data rows are output as arrays, not as objects.

## Example Usage {#example-usage}

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
                ["42", "hello", "[0,1]"],
                ["43", "hello", "[0,1,2]"],
                ["44", "hello", "[0,1,2,3]"]
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001572097,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## Format Settings {#format-settings}