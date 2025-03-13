---
title : JSONAsObject
slug : /en/interfaces/formats/JSONAsObject
keywords : [JSONAsObject]
input_format: true
output_format: false
alias: []
---

## Description

In this format, a single JSON object is interpreted as a single [JSON](/docs/en/sql-reference/data-types/newjson.md) value. If the input has several JSON objects (comma separated), they are interpreted as separate rows. If the input data is enclosed in square brackets, it is interpreted as an array of JSONs.

This format can only be parsed for a table with a single field of type [JSON](/docs/en/sql-reference/data-types/newjson.md). The remaining columns must be set to [`DEFAULT`](/docs/en/sql-reference/statements/create/table.md/#default) or [`MATERIALIZED`](/docs/en/sql-reference/statements/create/table.md/#materialized).

## Example Usage

### Basic Example

```sql title="Query"
SET allow_experimental_json_type = 1;
CREATE TABLE json_as_object (json JSON) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_object FORMAT JSONEachRow;
```

```response title="Response"
{"json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
{"json":{}}
{"json":{"any json stucture":"1"}}
```

### An array of JSON objects

```sql title="Query"
SET allow_experimental_json_type = 1;
CREATE TABLE json_square_brackets (field JSON) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsObject [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
SELECT * FROM json_square_brackets FORMAT JSONEachRow;
```

```response title="Response"
{"field":{"id":"1","name":"name1"}}
{"field":{"id":"2","name":"name2"}}
```

### Columns with default values

```sql title="Query"
SET allow_experimental_json_type = 1;
CREATE TABLE json_as_object (json JSON, time DateTime MATERIALIZED now()) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"any json stucture":1}
SELECT time, json FROM json_as_object FORMAT JSONEachRow
```

```response title="Response"
{"time":"2024-09-16 12:18:10","json":{}}
{"time":"2024-09-16 12:18:13","json":{"any json stucture":"1"}}
{"time":"2024-09-16 12:18:08","json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
```

## Format Settings

