-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

-- A one-field wrapper saves no column reads, so `optimizeUseRowWrappers`
-- must not route the read through it (the rewrite would only add the
-- per-row unpacking overhead of the `Row` format).

DROP TABLE IF EXISTS row_single_field;

CREATE TABLE row_single_field (
    id UInt64,
    a String,
    combined Row(a String) MATERIALIZED tuple(a)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_single_field (id, a) SELECT number, toString(number) FROM numbers(100);

SELECT countIf(explain LIKE '%__rowElement%') FROM (
    EXPLAIN actions = 1 SELECT a FROM row_single_field
    SETTINGS query_plan_use_row_wrappers = 1
);

SELECT count(), max(length(a)) FROM row_single_field
    SETTINGS query_plan_use_row_wrappers = 1;

DROP TABLE row_single_field;
