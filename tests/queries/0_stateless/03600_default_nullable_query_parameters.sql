-- Omitted query parameters with Nullable type should default to NULL.
-- Non-nullable parameters must still raise an error if not provided.

SELECT {p:Nullable(Int64)} IS NULL;
SELECT {p:Nullable(String)} IS NULL;
SELECT {p:Nullable(Float64)} IS NULL;
SELECT toTypeName({p:Nullable(Int64)});

SELECT {p:UInt8}; -- { serverError UNKNOWN_QUERY_PARAMETER }
