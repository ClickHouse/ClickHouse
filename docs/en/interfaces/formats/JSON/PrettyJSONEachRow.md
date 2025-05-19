---
alias: ['PrettyJSONLines', 'PrettyNDJSON']
description: 'Documentation for the PrettyJSONLines format'
input_format: false
keywords: ['PrettyJSONEachRow', 'PrettyJSONLines', 'PrettyNDJSON']
output_format: true
title: 'PrettyJSONEachRow'
---

| Input | Output | Alias                             |
|-------|--------|-----------------------------------|
| ✗     | ✔      | `PrettyJSONLines`, `PrettyNDJSON` |

## Description 

Differs from [JSONEachRow](./JSONEachRow.md) only in that JSON is pretty formatted with new line delimiters and 4 space indents.

## Example Usage 

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

## Format Settings 


