-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

-- A wrapper is one stream holding every wrapped field, so serving a narrow projection from it
-- decompresses far more bytes than the columns it replaces. `optimizeUseRowWrappers` must only
-- route the read through a wrapper when the requested columns cover at least half of its fields.

DROP TABLE IF EXISTS row_low_overlap;

CREATE TABLE row_low_overlap (
    id UInt64,
    a UInt32, b UInt32, c UInt32, d UInt32, e UInt32, f UInt32,
    bundle Row(a UInt32, b UInt32, c UInt32, d UInt32, e UInt32, f UInt32) MATERIALIZED tuple(a, b, c, d, e, f)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_low_overlap (id, a, b, c, d, e, f)
    SELECT number, number, number * 2, number * 3, number * 4, number * 5, number * 6 FROM numbers(100);

-- Two of six fields: no rewrite.
SELECT countIf(explain LIKE '%__rowElement%') FROM (
    EXPLAIN actions = 1 SELECT a, b FROM row_low_overlap
    SETTINGS query_plan_use_row_wrappers = 1
);

-- Three of six fields: the wrapper is used.
SELECT countIf(explain LIKE '%__rowElement%') > 0 FROM (
    EXPLAIN actions = 1 SELECT a, b, c FROM row_low_overlap
    SETTINGS query_plan_use_row_wrappers = 1
);

SELECT sum(a), sum(b) FROM row_low_overlap SETTINGS query_plan_use_row_wrappers = 1;
SELECT sum(a), sum(b), sum(c) FROM row_low_overlap SETTINGS query_plan_use_row_wrappers = 1;

DROP TABLE row_low_overlap;
