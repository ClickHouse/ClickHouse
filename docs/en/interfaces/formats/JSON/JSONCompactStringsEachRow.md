---
title : JSONCompactStringsEachRow
slug : /en/interfaces/formats/JSONCompactStringsEachRow
keywords : [JSONCompactStringsEachRow]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

Differs from [`JSONCompactEachRow`](./JSONCompactEachRow.md) only in that data fields are output as strings, not as typed JSON values.

## Example Usage

Example:

```json
["42", "hello", "[0,1]"]
["43", "hello", "[0,1,2]"]
["44", "hello", "[0,1,2,3]"]
```

## Format Settings

