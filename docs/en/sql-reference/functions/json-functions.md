---
description: 'Documentation for Json Functions'
sidebar_label: 'JSON'
slug: /sql-reference/functions/json-functions
title: 'JSON Functions'
doc_type: 'reference'
---

## Types of JSON functions {#types-of-functions}

There are two sets of functions to parse JSON:
- [`simpleJSON*` (`visitParam*`)](#simplejson-visitparam-functions) which is made for parsing a limited subset of JSON extremely fast.
- [`JSONExtract*`](#jsonextract-functions) which is made for parsing ordinary JSON.

### simpleJSON (visitParam) functions {#simplejson-visitparam-functions}

ClickHouse has special functions for working with simplified JSON. All these JSON functions are based on strong assumptions about what the JSON can be. They try to do as little as possible to get the job done as quickly as possible.

The following assumptions are made:

1.  The field name (function argument) must be a constant.
2.  The field name is somehow canonically encoded in JSON. For example: `simpleJSONHas('{"abc":"def"}', 'abc') = 1`, but `simpleJSONHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  Fields are searched for on any nesting level, indiscriminately. If there are multiple matching fields, the first occurrence is used.
4.  The JSON does not have space characters outside of string literals.

### JSONExtract functions {#jsonextract-functions}

These functions are based on [simdjson](https://github.com/lemire/simdjson), and designed for more complex JSON parsing requirements.

### Case-Insensitive JSONExtract Functions {#case-insensitive-jsonextract-functions}

These functions perform ASCII case-insensitive key matching when extracting values from JSON objects.
They work identically to their case-sensitive counterparts, except that object keys are matched without regard to case.
When multiple keys match with different cases, the first match is returned.

:::note
These functions may be less performant than their case-sensitive counterparts, so use the regular JSONExtract functions if possible.
:::

## JSONPath syntax {#jsonpath-syntax}

The `JSON_EXISTS`, `JSON_VALUE` and `JSON_QUERY` functions accept a JSONPath
expression as their second argument. ClickHouse implements a small subset of
JSONPath that is loosely based on the SQL/JSON standard (ISO/IEC TR 19075-6),
and is intentionally narrower than [RFC 9535](https://www.rfc-editor.org/rfc/rfc9535)
and other popular implementations. The supported operators are listed below.

The authoritative implementation lives in
[`src/Functions/JSONPath/Parsers`](https://github.com/ClickHouse/ClickHouse/tree/master/src/Functions/JSONPath/Parsers).

### Supported operators {#supported-operators}

| Syntax        | Description                                                                  | Example             |
|---------------|------------------------------------------------------------------------------|---------------------|
| `$`           | Root element. Every path must begin with it.                                 | `$`                 |
| `.key`        | Dot-notation member access.                                                   | `$.store.book`      |
| `['key']`     | Bracket-notation member access. Use it for keys containing spaces or special characters. Both single and double quotes are accepted. | `$['foo bar']`      |
| `[n]`         | Array element access by index (0-based).                                      | `$.items[0]`        |
| `[n, m, ...]` | Access to multiple array elements.                                           | `$.items[0, 2, 4]`  |
| `[n to m]`    | Array element range. The end index `m` is exclusive.                          | `$.items[0 to 3]`   |
| `[*]`         | Wildcard, matching all elements of an array.                                 | `$.items[*]`        |

Notes:

- Index ranges and the wildcard cannot be combined inside the same pair of brackets (for example, `[* , 0]` is invalid).
- In a range `[n to m]`, the start `n` must be strictly less than the end `m`, otherwise an exception is raised.
- A bare index `[n]` is equivalent to the range `[n to n+1]`.

### Unsupported operators {#unsupported-operators}

The following JSONPath operators, available in RFC 9535 or other
implementations, are **not** supported by ClickHouse:

| Syntax                | Description                                                              |
|-----------------------|-------------------------------------------------------------------------|
| `..`                  | Recursive descent.                                                       |
| `?(@.expr)`           | Filter expressions.                                                      |
| `[start:end:step]`    | Slice notation. Use `[n to m]` instead.                                  |
| `@`                   | Current node reference.                                                  |
| `[-n]`                | Negative (from-the-end) array indices.                                   |
| Script expressions    | Arithmetic or function expressions inside a path.                        |
| `ON ERROR` / `ON EMPTY` | Standard SQL/JSON clauses for controlling error and empty behavior.   |

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
