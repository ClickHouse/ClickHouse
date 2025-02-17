---
title : JSONAsString
slug : /en/interfaces/formats/JSONAsString
keywords : [JSONAsString]
input_format: true
output_format: false
alias: []
---

| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |


## Description

In this format, a single JSON object is interpreted as a single value. 
If the input has several JSON objects (which are comma separated), they are interpreted as separate rows. 
If the input data is enclosed in square brackets, it is interpreted as an array of JSON objects.

:::note
This format can only be parsed for a table with a single field of type [String](/docs/en/sql-reference/data-types/string.md). 
The remaining columns must be set to either [`DEFAULT`](/docs/en/sql-reference/statements/create/table.md/#default) or [`MATERIALIZED`](/docs/en/sql-reference/statements/create/table.md/#materialized), 
or be omitted. 
:::

Once you serialize the entire JSON object to a String you can use the [JSON functions](/docs/en/sql-reference/functions/json-functions.md) to process it.

## Example Usage

### Basic Example 

```sql title="Query"
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

```response title="Response"
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

### An array of JSON objects

```sql title="Query"
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

```response title="Response"
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

## Format Settings
