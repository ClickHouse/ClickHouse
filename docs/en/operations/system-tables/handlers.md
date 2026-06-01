---
description: 'Contains a list of all SQL-defined HTTP handlers created via CREATE HANDLER.'
keywords: ['system table', 'handlers']
slug: /operations/system-tables/handlers
title: 'system.handlers'
doc_type: 'reference'
---

## Description {#description}

Contains a list of all SQL-defined HTTP handlers created via [`CREATE HANDLER`](/sql-reference/statements/create/handler).

## Columns {#columns}

- `name` ([String](/sql-reference/data-types/string)) — Name of the handler.
- `protocol` ([Nullable(String)](/sql-reference/data-types/nullable)) — Composable protocol the handler is restricted to, or NULL for all http/https protocols.
- `url_match_type` ([String](/sql-reference/data-types/string)) — How the URL is matched: `exact`, `prefix` or `regexp`.
- `url` ([String](/sql-reference/data-types/string)) — The URL pattern that the handler matches.
- `methods` ([Array(String)](/sql-reference/data-types/array)) — Allowed HTTP methods.
- `type` ([String](/sql-reference/data-types/string)) — The handler type (only `query` is supported).
- `query` ([String](/sql-reference/data-types/string)) — The SQL query executed by the handler.
- `create_query` ([String](/sql-reference/data-types/string)) — The `CREATE HANDLER` statement.
