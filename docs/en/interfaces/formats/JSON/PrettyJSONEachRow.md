---
title: 'PrettyJSONEachRow'
slug: /interfaces/formats/PrettyJSONEachRow
keywords: ['PrettyJSONEachRow', 'PrettyJSONLines', 'PrettyNDJSON']
input_format: false
output_format: true
alias: ['PrettyJSONLines', 'PrettyNDJSON']
description: 'Documentation for the PrettyJSONLines format'
---

| Input | Output | Alias                             |
|-------|--------|-----------------------------------|
| ✗     | ✔      | `PrettyJSONLines`, `PrettyNDJSON` |

## Description {#description}

Differs from [JSONEachRow](./JSONEachRow.md) only in that JSON is pretty formatted with new line delimiters and 4 space indents.

## Example Usage {#example-usage}

```json
{
    "num": "42",
    "str": "hello",
    "arr": [
        "0",
        "1"
    ],
    "tuple": {
        "num": 42,
        "str": "world"
    }
}
{
    "num": "43",
    "str": "hello",
    "arr": [
        "0",
        "1",
        "2"
    ],
    "tuple": {
        "num": 43,
        "str": "world"
    }
}
```

## Format Settings {#format-settings}


