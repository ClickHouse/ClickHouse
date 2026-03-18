---
alias: []
description: 'Documentation for the LineAsStringWithNamesAndTypes format'
input_format: false
keywords: ['LineAsStringWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/LineAsStringWithNamesAndTypes
title: 'LineAsStringWithNamesAndTypes'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `LineAsStringWithNames` format is similar to the [`LineAsString`](./LineAsString.md) format 
but prints two header rows: one with column names, the other with types.

## Example Usage {#example-usage}

```sql
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNamesAndTypes;
```

```response title="Response"
name    value
String    Int32
John    30
Jane    25
Peter    35
```

## Format Settings {#format-settings}
