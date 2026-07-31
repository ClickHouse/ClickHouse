-- `length(FixedString(N))` is computed as a single `ColumnConst(N)` because every row
-- of a `FixedString(N)` column has exactly N bytes. When that const-collapsed result was
-- wrapped back into a `Nullable` through `wrapInNullable`, only the null bit of row 0
-- was kept and was broadcast to every row, so any row whose null-ness differed from
-- row 0 was reported with the wrong null value.

-- First row not-null, the rest NULL: each NULL row used to wrongly report length 10.
SELECT n, length(x) FROM (
    SELECT
        number AS n,
        multiIf(number = 0, CAST('hello\0\0\0\0\0' AS Nullable(FixedString(10))), NULL) AS x
    FROM numbers(4)
) ORDER BY n;

-- First row NULL, second row not-null: the non-null row used to wrongly report NULL.
SELECT n, length(x) FROM (
    SELECT
        number AS n,
        multiIf(
            number = 0, NULL,
            number = 1, CAST('hello\0\0\0\0\0' AS Nullable(FixedString(10))),
            NULL) AS x
    FROM numbers(4)
) ORDER BY n;

-- All NULL: result is uniformly NULL (this was already correct, kept for completeness).
SELECT n, length(x) FROM (
    SELECT number AS n, CAST(NULL AS Nullable(FixedString(10))) AS x FROM numbers(4)
) ORDER BY n;

-- No NULL: result is uniformly the fixed size (also already correct).
SELECT n, length(x) FROM (
    SELECT
        number AS n,
        CAST(concat('row', toString(number), '\0\0\0\0\0') AS Nullable(FixedString(10))) AS x
    FROM numbers(4)
) ORDER BY n;
