-- LowCardinality(Nullable(T)) query parameters should also default to NULL when omitted.
-- https://github.com/ClickHouse/ClickHouse/issues/99805

SELECT {p:LowCardinality(Nullable(String))} IS NULL;
SELECT {p:LowCardinality(Nullable(Int64))} IS NULL;
SELECT toTypeName({p:LowCardinality(Nullable(String))});
