SET cast_keep_nullable = 0;

-- https://github.com/ClickHouse/ClickHouse/issues/5818#issuecomment-619628445
SELECT CAST(CAST(NULL AS Nullable(String)) AS Nullable(Enum8('Hello' = 1)));
SELECT CAST(CAST(NULL AS Nullable(FixedString(1))) AS Nullable(Enum8('Hello' = 1)));

-- empty string still not acceptable
SELECT CAST(CAST('' AS Nullable(String)) AS Nullable(Enum8('Hello' = 1))); -- { serverError 36; }
SELECT CAST(CAST('' AS Nullable(FixedString(1))) AS Nullable(Enum8('Hello' = 1))); -- { serverError 36; }

-- non-Nullable Enum() still not acceptable
SELECT CAST(CAST(NULL AS Nullable(String)) AS Enum8('Hello' = 1)); -- { serverError 349; }
SELECT CAST(CAST(NULL AS Nullable(FixedString(1))) AS Enum8('Hello' = 1)); -- { serverError 349; }
