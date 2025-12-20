-- { echo ON }

DROP TABLE IF EXISTS t;

CREATE TABLE t(
    x UInt64,
    y String
)
ENGINE = MergeTree()
ORDER BY x SETTINGS index_granularity = 999999999, index_granularity_bytes = 99999999999, use_const_adaptive_granularity = 0, min_bytes_for_wide_part = 0;

ALTER TABLE t ADD PROJECTION p1 (SELECT x, y ORDER BY x) WITH SETTINGS (index_granularity = 0); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t ADD PROJECTION p2 (SELECT x ORDER BY x) WITH SETTINGS (index_granularity_bytes = 512); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t ADD PROJECTION p2 (SELECT x ORDER BY x) WITH SETTINGS (index_granularity_bytes = 0); -- { serverError BAD_ARGUMENTS }

ALTER TABLE t ADD PROJECTION p3 (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 2, index_granularity_bytes = 999999999);
ALTER TABLE t ADD PROJECTION p4 (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 9999999999, index_granularity_bytes = 4096);

INSERT INTO t SETTINGS max_insert_block_size = 2000000 SELECT number, toString(number) FROM numbers(10000) SETTINGS max_block_size = 2000000;

SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 't' AND name = 'p3';
SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 't' AND name = 'p4';
SELECT name, settings FROM system.projections WHERE database = currentDatabase() AND table = 't' ORDER BY name;

DROP TABLE t;

CREATE TABLE t
(
    x UInt64,
    y String
)
ENGINE = MergeTree()
ORDER BY x
SETTINGS index_granularity_bytes = 0;

-- Non-adaptive merge tree tables cannot have projections with settings
ALTER TABLE t ADD PROJECTION p1 (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 2); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t ADD PROJECTION p2 (SELECT x ORDER BY x) WITH SETTINGS (index_granularity_bytes = 4096); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t ADD PROJECTION p3 (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 2, index_granularity_bytes = 4096); -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t;
