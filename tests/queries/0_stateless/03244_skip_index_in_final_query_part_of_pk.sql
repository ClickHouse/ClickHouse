-- Testcase for bug #85897
-- If skip index is part of primary key, then optimization 'use_skip_indexes_if_final_exact_mode' should
-- not perform additional primary key intersection expand step.

SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
     id1 UInt32,
     id2 UInt32,
     v UInt32,
    INDEX id2_idx id2 TYPE minmax, -- part of primary key
    INDEX v_idx v TYPE minmax,     -- not part of primary key
)
ENGINE = ReplacingMergeTree
ORDER BY (id1, id2)
SETTINGS index_granularity = 4;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number/100, number, number FROM numbers(1000);
INSERT INTO tab SELECT number/50, number, number * 5 FROM numbers(1000);

SELECT 'Expand should be done because of column v skip index';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM tab FINAL WHERE v = 222
)
WHERE explain ILIKE '%PrimaryKeyExpand%';

SELECT 'Expand should not be done because id2 is part of skip index';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM tab FINAL WHERE id2 = 222
)
WHERE explain ILIKE '%PrimaryKeyExpand%';

DROP TABLE tab;

-- Test from fuzzer : https://github.com/ClickHouse/ClickHouse/issues/89387
DROP TABLE IF EXISTS t0;

CREATE TABLE t0
(
    c0 Int64,
    INDEX i1 c0 TYPE set(0)
) ENGINE = SummingMergeTree() PARTITION BY (c0) ORDER BY (c0);

INSERT INTO TABLE t0 (c0) SELECT number FROM numbers(10);

SELECT rank() OVER () FROM t0 FINAL WHERE t0.c0 > 0.1 FORMAT null;

DROP TABLE t0;

