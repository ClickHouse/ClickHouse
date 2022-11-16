DROP TABLE IF EXISTS t_modify_from_lc_1;
DROP TABLE IF EXISTS t_modify_from_lc_2;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE t_modify_from_lc_1
(
    id UInt64,
    a LowCardinality(UInt32) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

CREATE TABLE t_modify_from_lc_2
(
    id UInt64,
    a LowCardinality(UInt32) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_modify_from_lc_1 SELECT number, number FROM numbers(100000);
INSERT INTO t_modify_from_lc_2 SELECT number, number FROM numbers(100000);

OPTIMIZE TABLE t_modify_from_lc_1 FINAL;
OPTIMIZE TABLE t_modify_from_lc_2 FINAL;

ALTER TABLE t_modify_from_lc_1 MODIFY COLUMN a UInt32;

-- Check that dictionary of LowCardinality is actually
-- dropped and total size on disk is reduced.
WITH groupArray((table, bytes))::Map(String, UInt64) AS stats
SELECT
    length(stats), stats['t_modify_from_lc_1'] < stats['t_modify_from_lc_2']
FROM
(
    SELECT table, sum(bytes_on_disk) AS bytes FROM system.parts
    WHERE database = currentDatabase() AND table LIKE 't_modify_from_lc%' AND active
    GROUP BY table
);

DROP TABLE IF EXISTS t_modify_from_lc_1;
DROP TABLE IF EXISTS t_modify_from_lc_2;
