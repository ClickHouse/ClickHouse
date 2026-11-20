-- Regression test: `*` (and other column matchers) in PARTITION BY / ORDER BY
-- expanded to multiple columns by the Analyzer and crashed in
-- `registerStorageMergeTree` because `column_names` (one per AST child) was
-- shorter than `data_types` (one per resolved output column).

DROP TABLE IF EXISTS t_wildcard_partition_key;

CREATE TABLE t_wildcard_partition_key
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
PARTITION BY (*, b * b)
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_wildcard_partition_key
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (*, b * b); -- { serverError BAD_ARGUMENTS }

-- A matcher that resolves to exactly ONE column must also be rejected: the
-- resolved-output count then matches the key-element count, so a count-based
-- guard would pass while `column_names` keeps the literal matcher text
-- (e.g. `COLUMNS('^a$')`) instead of the matched column, leaving inconsistent
-- key metadata. Matchers are rejected syntactically regardless of match count.

CREATE TABLE t_wildcard_partition_key
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY COLUMNS('^a$'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_wildcard_partition_key
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
PARTITION BY COLUMNS('^a$')
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- `*` on a single-column table (the asterisk resolves to one column).
CREATE TABLE t_wildcard_partition_key
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY *; -- { serverError BAD_ARGUMENTS }

-- A matcher nested inside a function call is rejected too.
CREATE TABLE t_wildcard_partition_key
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY tuple(COLUMNS('^a$')); -- { serverError BAD_ARGUMENTS }
