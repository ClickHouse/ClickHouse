---
alias: []
description: 'Documentation for the JSONCompactStringsEachRow format'
input_format: true
keywords: ['JSONCompactStringsEachRow']
output_format: true
slug: /interfaces/formats/JSONCompactStringsEachRow
title: 'JSONCompactStringsEachRow'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from [`JSONCompactEachRow`](./JSONCompactEachRow.md) only in that data fields are output as strings, not as typed JSON values.

## Example Usage {#example-usage}

Example:

```json
["42", "hello", "[0,1]"]
["43", "hello", "[0,1,2]"]
["44", "hello", "[0,1,2,3]"]
```

## Format Settings {#format-settings}

