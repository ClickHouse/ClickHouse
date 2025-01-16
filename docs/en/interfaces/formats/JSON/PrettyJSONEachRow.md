---
title : PrettyJSONEachRow
slug : /en/interfaces/formats/PrettyJSONEachRow
keywords : [PrettyJSONEachRow]
---

## Description

Differs from JSONEachRow only in that JSON is pretty formatted with new line delimiters and 4 space indents. Suitable only for output.

Alias: `PrettyJSONLines`, `PrettyNDJSON`

## Example Usage

Example

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


