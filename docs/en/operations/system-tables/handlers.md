---
description: 'Contains a list of all SQL-defined HTTP handlers created via CREATE HANDLER.'
keywords: ['system table', 'handlers']
sidebar_label: 'handlers'
sidebar_position: 50
slug: /operations/system-tables/handlers
title: 'system.handlers'
doc_type: 'reference'
---

## Description {#description}

Contains a list of all SQL-defined HTTP handlers created via [`CREATE HANDLER`](/sql-reference/statements/create/handler).

Reading this table requires the `SHOW HANDLERS` privilege; without it the table appears empty. Secrets that
may be embedded in the handler's query (for example credentials passed to a table function) are shown in the
`query` and `create_query` columns only when the user is allowed to see secrets (the
`display_secrets_in_show_and_select` server setting and `format_display_secrets_in_show_and_select` format
setting are enabled and the user holds the `displaySecretsInShowAndSelect` privilege); otherwise they are
masked, exactly as in `SHOW CREATE TABLE`.

## Columns {#columns}

- `name` ([String](/sql-reference/data-types/string)) — Name of the handler.
- `protocol` ([Nullable(String)](/sql-reference/data-types/nullable)) — Composable protocol the handler is restricted to, or NULL for all http/https protocols.
- `url_match_type` ([String](/sql-reference/data-types/string)) — How the URL is matched: `exact`, `prefix` or `regexp`.
- `url` ([String](/sql-reference/data-types/string)) — The URL pattern that the handler matches.
- `methods` ([Array(String)](/sql-reference/data-types/array)) — Allowed HTTP methods.
- `type` ([String](/sql-reference/data-types/string)) — The handler type (only `query` is supported).
- `query` ([String](/sql-reference/data-types/string)) — The SQL query executed by the handler. Secrets are masked unless the user is allowed to see them.
- `create_query` ([String](/sql-reference/data-types/string)) — The `CREATE HANDLER` statement. Secrets are masked unless the user is allowed to see them.
