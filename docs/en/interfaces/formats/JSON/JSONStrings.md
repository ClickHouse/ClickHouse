---
alias: []
description: 'Documentation for the JSONStrings format'
input_format: true
keywords: ['JSONStrings']
output_format: true
slug: /interfaces/formats/JSONStrings
title: 'JSONStrings'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [JSON](./JSON.md) format only in that data fields are output as strings, not as typed JSON values.

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
        [
                {
                        "num": "42",
                        "str": "hello",
                        "arr": "[0,1]"
                },
                {
                        "num": "43",
                        "str": "hello",
                        "arr": "[0,1,2]"
                },
                {
                        "num": "44",
                        "str": "hello",
                        "arr": "[0,1,2,3]"
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001403233,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## Format Settings {#format-settings}

