---
title : JSONEachRow
slug : /en/interfaces/formats/JSONEachRow
keywords : [JSONEachRow]
---

## Description

In this format, ClickHouse outputs each row as a separated, newline-delimited JSON Object. Alias: `JSONLines`, `NDJSON`.

## Example Usage

Example:

```json
{"num":42,"str":"hello","arr":[0,1]}
{"num":43,"str":"hello","arr":[0,1,2]}
{"num":44,"str":"hello","arr":[0,1,2,3]}
```

While importing data columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.

## Format Settings