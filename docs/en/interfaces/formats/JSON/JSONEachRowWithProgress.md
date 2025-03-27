---
title : JSONEachRowWithProgress
slug : /en/interfaces/formats/JSONEachRowWithProgress
keywords : [JSONEachRowWithProgress]
input_format: false
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description

Differs from [`JSONEachRow`](./JSONEachRow.md)/[`JSONStringsEachRow`](./JSONStringsEachRow.md) in that ClickHouse will also yield progress information as JSON values.

## Example Usage

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## Format Settings

