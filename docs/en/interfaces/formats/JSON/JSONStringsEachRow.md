---
title : JSONStringsEachRow
slug : /en/interfaces/formats/JSONStringsEachRow
keywords : [JSONStringsEachRow]
input_format: false
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description

Differs from the [`JSONEachRow`](./JSONEachRow.md) only in that data fields are output in strings, not in typed JSON values.

## Example Usage

```json
{"num":"42","str":"hello","arr":"[0,1]"}
{"num":"43","str":"hello","arr":"[0,1,2]"}
{"num":"44","str":"hello","arr":"[0,1,2,3]"}
```

## Format Settings

