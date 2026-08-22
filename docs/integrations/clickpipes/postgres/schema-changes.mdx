---
title: 'Schema changes propagation support'
slug: /integrations/clickpipes/postgres/schema-changes
description: 'Page describing schema change types detectable by ClickPipes in the source tables'
doc_type: 'reference'
keywords: ['clickpipes', 'postgresql', 'cdc', 'data ingestion', 'real-time sync']
integration:
  - support_level: 'core'
  - category: 'clickpipes'
---

ClickPipes for Postgres can detect schema changes in the source tables and, in some cases, automatically propagate the changes to the destination tables. The way each DDL operation is handled is documented below:

[//]: # "TODO Extend this page with behavior on rename, data type changes, and truncate + guidance on how to handle incompatible schema changes."

| Schema Change Type                                                                  | Behaviour                             |
| ----------------------------------------------------------------------------------- | ------------------------------------- |
| Adding a new column (`ALTER TABLE ADD COLUMN ...`)                                  | Propagated automatically once the table gets an insert/update/delete. The new columns will be populated for all rows replicated after the schema change                                                   |
| Adding a new column with a default value (`ALTER TABLE ADD COLUMN ... DEFAULT ...`) | Propagated automatically once the table gets an insert/update/delete. [Supported defaults](#default-values-for-added-columns) are also added to the ClickHouse column, so they apply to rows already in ClickHouse as well as rows replicated after the schema change. |
| Dropping an existing column (`ALTER TABLE DROP COLUMN ...`)                         | Detected, but **not** propagated. The dropped columns will be populated with `NULL` for all rows replicated after the schema change                                                                |

Note that column addition will be propagated at the end of a batch's sync, which could occur after the sync interval or pull batch size is reached. More information on controlling syncs [here](/integrations/clickpipes/postgres/controlling-sync)

## Default values for added columns {#default-values-for-added-columns}

For an added column, ClickPipes propagates a default only when it can translate the value safely to ClickHouse. When a default is propagated, ClickHouse uses it when reading data parts that predate the new column. As a result, rows already present in ClickHouse show the same value that PostgreSQL uses for rows that predate `ADD COLUMN`; a full table refresh is not required.

For PostgreSQL, ClickPipes reads the value PostgreSQL stored for pre-existing rows, rather than the column's current default expression. This matters when the default is an expression: a non-volatile expression such as `now()` is evaluated by PostgreSQL when the column is added, and ClickPipes propagates that resulting value, not the expression. Later `SET DEFAULT` or `DROP DEFAULT` operations do not change the default used for existing rows and are not propagated to ClickHouse.

The following values are propagated:

- Boolean and finite numeric values.
- Text values, including `varchar`, `char`, enums, UUIDs, JSON/JSONB, `hstore`, network addresses, and dates.
- Timestamps. Timestamps with time zones are normalized to UTC.

Defaults are omitted when their representation is not safely portable to ClickHouse, including `NULL`, non-finite numeric values, `bytea`, arrays, intervals, time-of-day values, and geometric types. The column is still added, but ClickHouse uses its normal type default for parts that predate the column. A full table refresh is required if those existing rows must contain the source default.
