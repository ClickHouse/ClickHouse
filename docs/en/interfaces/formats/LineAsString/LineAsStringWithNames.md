---
title : LineAsStringWithNames
slug: /interfaces/formats/LineAsStringWithNames
keywords : [LineAsStringWithNames]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description

The `LineAsStringWithNames` format is similar to the [`LineAsString`](./LineAsString.md) format but prints the header row with column names.

## Example Usage

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
name	value
John	30
Jane	25
Peter	35
```

## Format Settings