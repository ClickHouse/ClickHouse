---
alias: []
description: 'Documentation for the LineAsStringWithNames format'
input_format: true
keywords: ['LineAsStringWithNames']
output_format: true
slug: /interfaces/formats/LineAsStringWithNames
title: 'LineAsStringWithNames'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `LineAsStringWithNames` format is similar to the [`LineAsString`](./LineAsString.md) format but prints the header row with column names.

## Example Usage {#example-usage}

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNames;
```

```response title="Response"
name    value
John    30
Jane    25
Peter    35
```

## Format Settings {#format-settings}