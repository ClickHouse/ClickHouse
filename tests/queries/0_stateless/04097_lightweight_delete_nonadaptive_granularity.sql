-- Tags: no-replicated-database

-- Lightweight DELETE on a table with non-adaptive index granularity (index_granularity_bytes = 0)
-- should not cause a "Incorrect mark rows" logical error after DETACH/ATTACH.
-- https://github.com/ClickHouse/ClickHouse/issues/98585

DROP TABLE IF EXISTS t_lightweight_delete_nonadaptive;

CREATE TABLE t_lightweight_delete_nonadaptive (c0 Int)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS index_granularity_bytes = 0;

INSERT INTO TABLE t_lightweight_delete_nonadaptive (c0) VALUES (1);

DETACH TABLE t_lightweight_delete_nonadaptive SYNC;
ATTACH TABLE t_lightweight_delete_nonadaptive;

DELETE FROM t_lightweight_delete_nonadaptive WHERE TRUE;

SELECT count() FROM t_lightweight_delete_nonadaptive;

DROP TABLE t_lightweight_delete_nonadaptive;
