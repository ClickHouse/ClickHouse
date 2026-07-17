-- Tags: no-random-merge-tree-settings

-- Primary key index analysis for a reverse (`DESC`) sort key column must narrow the selection
-- to the granules matching the range condition instead of the whole slice of the leading key
-- value. 29333 of the 80000 `org_m` rows match `dt > 2026-06-20`, i.e. ~230 of ~626 granules
-- of the `org_m` slice; the threshold below leaves generous slack and only catches selecting
-- (almost) the whole slice.

DROP TABLE IF EXISTS t_reverse_key_granules;

CREATE TABLE t_reverse_key_granules (org String, dt DateTime, id UInt64)
ENGINE = MergeTree
ORDER BY (org, dt DESC, id)
SETTINGS index_granularity = 128;

INSERT INTO t_reverse_key_granules SELECT 'org_a', toDateTime('2026-06-01') + intDiv(number * 2592000, 20000), number FROM numbers(20000);
INSERT INTO t_reverse_key_granules SELECT 'org_m', toDateTime('2026-06-01') + intDiv(number * 2592000, 80000), number FROM numbers(80000);
INSERT INTO t_reverse_key_granules SELECT 'org_z', toDateTime('2026-06-01') + intDiv(number * 2592000, 20000), number FROM numbers(20000);

OPTIMIZE TABLE t_reverse_key_granules FINAL;

SELECT count() FROM t_reverse_key_granules WHERE org = 'org_m' AND dt > toDateTime('2026-06-20');

SELECT if(selected <= 300, 'OK', 'selected too many granules: ' || toString(selected) || '/' || toString(total))
FROM
(
    SELECT
        (splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1])::UInt64 AS selected,
        (splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2])::UInt64 AS total
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_reverse_key_granules WHERE org = 'org_m' AND dt > toDateTime('2026-06-20') SETTINGS use_lightweight_primary_key_index_analysis = 1)
    WHERE explain LIKE '%Granules: %/%'
);

SELECT if(selected <= 300, 'OK', 'selected too many granules: ' || toString(selected) || '/' || toString(total))
FROM
(
    SELECT
        (splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1])::UInt64 AS selected,
        (splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2])::UInt64 AS total
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_reverse_key_granules WHERE org = 'org_m' AND dt > toDateTime('2026-06-20') SETTINGS use_lightweight_primary_key_index_analysis = 0)
    WHERE explain LIKE '%Granules: %/%'
);

DROP TABLE t_reverse_key_granules;
