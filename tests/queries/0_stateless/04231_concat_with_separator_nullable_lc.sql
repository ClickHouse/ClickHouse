-- `concatWithSeparator(sep)` with only the separator argument collapses to
-- `ColumnConst('')`. When `sep` is `LowCardinality(Nullable(...))`, the default null
-- handler used to broadcast row 0's null bit to every row through `wrapInNullable`,
-- so identical inputs gave different results depending on whether they reached the
-- function via a column or via a `ColumnConst`. Same bug as `length(FixedString)`,
-- different surface.

-- Column path: row 1 is not-null, rest are NULL. The non-null row must produce ''.
SELECT n, concatWithSeparator(x) FROM (
    SELECT
        number AS n,
        multiIf(number = 1, CAST(repeat('a', 10) AS LowCardinality(Nullable(FixedString(10)))), NULL) AS x
    FROM numbers(4)
) ORDER BY n;

-- Const not-null: must produce ''.
SELECT concatWithSeparator(CAST(repeat('a', 10) AS LowCardinality(Nullable(FixedString(10)))));

-- Const NULL: must produce NULL.
SELECT concatWithSeparator(CAST(NULL AS LowCardinality(Nullable(FixedString(10)))));

-- Materialized (single-row column) not-null: must produce '' just like the const above.
SELECT concatWithSeparator(materialize(CAST(repeat('a', 10) AS LowCardinality(Nullable(FixedString(10))))));

-- Materialized (single-row column) NULL: must produce NULL.
SELECT concatWithSeparator(materialize(CAST(NULL AS LowCardinality(Nullable(FixedString(10))))));
