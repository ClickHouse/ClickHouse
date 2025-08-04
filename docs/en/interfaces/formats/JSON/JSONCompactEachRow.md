---
alias: []
description: 'Documentation for the JSONCompactEachRow format'
input_format: true
keywords: ['JSONCompactEachRow']
output_format: true
slug: /interfaces/formats/JSONCompactEachRow
title: 'JSONCompactEachRow'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from [`JSONEachRow`](./JSONEachRow.md) only in that data rows are output as arrays, not as objects.

## Example Usage {#example-usage}

Example:

```json
[42, "hello", [0,1]]
[43, "hello", [0,1,2]]
[44, "hello", [0,1,2,3]]
```

## Format Settings {#format-settings}

