-- Testcase for fixing https://github.com/ClickHouse/ClickHouse/issues/85897
-- If skip index is part of primary key, then optimization 'use_skip_indexes_if_final_exact_mode' should
-- not perform additional primary key intersection expand step.

SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
     id1 UInt32,
     id2 UInt32,
     v UInt32,
    INDEX id2_idx id2 TYPE minmax,
    INDEX v_idx v TYPE minmax,
)
ENGINE = ReplacingMergeTree
ORDER BY (id1, id2)
SETTINGS index_granularity=4;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number/100, number, number FROM numbers(1000);
INSERT INTO tab SELECT number/50, number, number * 5 FROM numbers(1000);

-- Expand should be done because of column 'v' skip index
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM tab FINAL WHERE v = 222
)
WHERE explain ILIKE '%PrimaryKeyExpand%';

-- Expand should not be done because id2 is part of primary key
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM tab FINAL WHERE id2 = 222
)
WHERE explain ILIKE '%PrimaryKeyExpand%';

DROP TABLE tab;
