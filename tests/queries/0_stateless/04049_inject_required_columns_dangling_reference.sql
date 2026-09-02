-- Test for dangling reference bug in injectRequiredColumns.
-- When a column is missing from a part and has a DEFAULT expression,
-- injectRequiredColumnsRecursively adds dependencies via emplace_back,
-- which can reallocate the columns vector and invalidate references into it.
-- https://github.com/ClickHouse/ClickHouse/pull/99679

DROP TABLE IF EXISTS t_inject_columns_dangling_ref;

CREATE TABLE t_inject_columns_dangling_ref (key UInt64) ENGINE = MergeTree ORDER BY key;

INSERT INTO t_inject_columns_dangling_ref SELECT number FROM numbers(100);

-- Add many columns with DEFAULT expressions that reference `key`.
-- When these columns are read from existing parts (where they don't exist),
-- injectRequiredColumnsRecursively will inject `key` via emplace_back on the columns vector.
-- With enough columns, the vector will reallocate mid-iteration, causing the dangling reference.
ALTER TABLE t_inject_columns_dangling_ref
    ADD COLUMN c1 UInt64 DEFAULT key + 1,
    ADD COLUMN c2 UInt64 DEFAULT key + 2,
    ADD COLUMN c3 UInt64 DEFAULT key + 3,
    ADD COLUMN c4 UInt64 DEFAULT key + 4,
    ADD COLUMN c5 UInt64 DEFAULT key + 5,
    ADD COLUMN c6 UInt64 DEFAULT key + 6,
    ADD COLUMN c7 UInt64 DEFAULT key + 7,
    ADD COLUMN c8 UInt64 DEFAULT key + 8,
    ADD COLUMN c9 UInt64 DEFAULT key + 9,
    ADD COLUMN c10 UInt64 DEFAULT key + 10,
    ADD COLUMN c11 UInt64 DEFAULT key + 11,
    ADD COLUMN c12 UInt64 DEFAULT key + 12,
    ADD COLUMN c13 UInt64 DEFAULT key + 13,
    ADD COLUMN c14 UInt64 DEFAULT key + 14,
    ADD COLUMN c15 UInt64 DEFAULT key + 15,
    ADD COLUMN c16 UInt64 DEFAULT key + 16;

SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16
FROM t_inject_columns_dangling_ref
ORDER BY c1
LIMIT 5;

DROP TABLE t_inject_columns_dangling_ref;
