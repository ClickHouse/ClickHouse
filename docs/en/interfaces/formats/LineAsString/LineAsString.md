---
title : LineAsString
slug: /interfaces/formats/LineAsString
keywords : [LineAsString]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `LineAsString` format interprets every line of input data as a single string value. 
This format can only be parsed for a table with a single field of type [String](/docs/sql-reference/data-types/string.md). 
The remaining columns must be set to [`DEFAULT`](/docs/sql-reference/statements/create/table.md/#default), [`MATERIALIZED`](/docs/sql-reference/statements/create/table.md/#materialized), or omitted.

## Example Usage {#example-usage}

```sql title="Query"
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

```text title="Response"
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

## Format Settings {#format-settings}