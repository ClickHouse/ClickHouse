---
toc_priority: 42
toc_title: DESCRIBE
---

# DESCRIBE TABLE Statement {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Returns the following `String` type columns:

-   `name` — Column name.
-   `type`— Column type.
-   `default_type` — Clause that is used in [default expression](../../sql-reference/statements/create/table.md#create-default-values) (`DEFAULT`, `MATERIALIZED` or `ALIAS`). Column contains an empty string, if the default expression isn’t specified.
-   `default_expression` — Value specified in the `DEFAULT` clause.
-   `comment_expression` — Comment text.

Nested data structures are output in “expanded” format. Each column is shown separately, with the name after a dot.
