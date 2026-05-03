---
description: 'Documentation for the JSON AST dialect and the parseQueryToJSON / formatQueryFromJSON functions'
sidebar_label: 'JSON AST dialect'
sidebar_position: 11
slug: /sql-reference/json-ast-dialect
title: 'JSON AST dialect'
doc_type: 'reference'
---

ClickHouse can serialize a query's parsed Abstract Syntax Tree (AST) to JSON,
deserialize it back, and accept the JSON form directly as a query in place of SQL text.

This is useful for tools that want to inspect or transform queries programmatically
without going through the SQL grammar.

## Functions {#functions}

### `parseQueryToJSON` {#parsequerytojson}

Parses a SQL query string and returns its AST as a JSON string.

```sql
SELECT parseQueryToJSON('SELECT 1');
```

### `formatQueryFromJSON` {#formatqueryfromjson}

Takes a JSON AST (as produced by `parseQueryToJSON`) and formats it back into a
SQL query string.

```sql
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t WHERE x > 1'));
```

When called with a second argument, it preserves comments, whitespace, and other
trivia from the original query for tokens that did not change:

```sql
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t'), 'SELECT /* comment */ a FROM t');
```

## `clickhouse_json` dialect {#clickhouse-json-dialect}

When the [`dialect`](/operations/settings/settings#dialect) setting is set to
`clickhouse_json`, the server treats the incoming query as a JSON AST (the
output of `parseQueryToJSON`) instead of SQL text. The `SET` query is still
recognized in plain form so that the dialect can be switched back.

This dialect is experimental and gated by the
[`allow_experimental_json_ast_dialect`](/operations/settings/settings#allow_experimental_json_ast_dialect)
setting.

Example:

```sql
SET allow_experimental_json_ast_dialect = 1;
SET dialect = 'clickhouse_json';

-- Subsequent queries on this session are parsed as JSON AST:
{"type":"SelectWithUnionQuery", ...}
```
