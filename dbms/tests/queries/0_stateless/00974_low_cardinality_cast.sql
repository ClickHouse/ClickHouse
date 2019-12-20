SELECT CAST('Hello' AS LowCardinality(Nullable(String)));
SELECT CAST(Null AS LowCardinality(Nullable(String)));
SELECT CAST(CAST('Hello' AS LowCardinality(Nullable(String))) AS String);
SELECT CAST(CAST(Null AS LowCardinality(Nullable(String))) AS String); -- { serverError 349 }
SELECT CAST(CAST('Hello' AS Nullable(String)) AS String);
SELECT CAST(CAST(Null AS Nullable(String)) AS String); -- { serverError 349 }
