---
title: 'JSONCompactStringsEachRow'
slug: /interfaces/formats/JSONCompactStringsEachRow
keywords: ['JSONCompactStringsEachRow']
input_format: true
output_format: true
alias: []
description: 'Documentation for the JSONCompactStringsEachRow format'
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

