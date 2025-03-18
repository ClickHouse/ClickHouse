---
alias: []
description: 'Documentation for the JSONStringsEachRow format'
input_format: false
keywords: ['JSONStringsEachRow']
output_format: true
slug: /interfaces/formats/JSONStringsEachRow
title: 'JSONStringsEachRow'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Differs from the [`JSONEachRow`](./JSONEachRow.md) only in that data fields are output in strings, not in typed JSON values.

## Example Usage {#example-usage}

```json
{"num":"42","str":"hello","arr":"[0,1]"}
{"num":"43","str":"hello","arr":"[0,1,2]"}
{"num":"44","str":"hello","arr":"[0,1,2,3]"}
```

## Format Settings {#format-settings}

