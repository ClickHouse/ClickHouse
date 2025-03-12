---
title : JSONCompactEachRow
slug : /en/interfaces/formats/JSONCompactEachRow
keywords : [JSONCompactEachRow]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

Differs from [`JSONEachRow`](./JSONEachRow.md) only in that data rows are output as arrays, not as objects.

## Example Usage

Example:

```json
[42, "hello", [0,1]]
[43, "hello", [0,1,2]]
[44, "hello", [0,1,2,3]]
```

## Format Settings

