---
description: 'Documentation for the JSONStringsEachRowWithProgress format'
keywords: ['JSONStringsEachRowWithProgress']
slug: /interfaces/formats/JSONStringsEachRowWithProgress
title: 'JSONStringsEachRowWithProgress'
---

## Description {#description}

Differs from `JSONEachRow`/`JSONStringsEachRow` in that ClickHouse will also yield progress information as JSON values.

## Example Usage {#example-usage}

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## Format Settings {#format-settings}
