# MaterializedView

Used for implementing materialized views (for more information, see [CREATE TABLE](../../query_language/create.md#query_language-queries-create_table)). For storing data, it uses a different engine that was specified when creating the view. When reading from a table, it just uses this engine.

