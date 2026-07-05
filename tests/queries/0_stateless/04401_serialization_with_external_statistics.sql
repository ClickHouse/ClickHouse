-- Tests for choosing sparse serialization from estimates.
--
-- (1) The serialization kind (sparse) is chosen from the always-on lightweight estimates sampled from
--     the written data, even when the statistics framework is disabled
--     (materialize_statistics_on_insert = 0).
-- (2) A column with an explicit `basic` statistic gets its default count from the statistic: it
--     overrides the sampled count when `serialization.json` is written, and merges re-decide the kind
--     from the per-part counts.

SET optimize_trivial_insert_select = 1;
SET allow_experimental_statistics = 1;

DROP TABLE IF EXISTS t_ser_ext_stats;

-- (1) Always-on lightweight path: no external statistics, statistics framework disabled.
CREATE TABLE t_ser_ext_stats (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

SYSTEM STOP MERGES t_ser_ext_stats;

SET materialize_statistics_on_insert = 0;
INSERT INTO t_ser_ext_stats SELECT number, if(number % 100 = 0, 'x', '') FROM numbers(100000);

SELECT 'always_on', serialization_kind
FROM system.parts_columns
WHERE table = 't_ser_ext_stats' AND database = currentDatabase() AND column = 's' AND active
ORDER BY name;

DROP TABLE t_ser_ext_stats;

-- (2) External `basic` statistic: its default count overrides the sampled one; merges re-decide the
--     kind from the per-part counts.
DROP TABLE IF EXISTS t_ser_ext_stats2;

CREATE TABLE t_ser_ext_stats2 (id UInt64, s String STATISTICS(basic))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

SYSTEM STOP MERGES t_ser_ext_stats2;
SET materialize_statistics_on_insert = 1;

INSERT INTO t_ser_ext_stats2 SELECT number, if(number % 100 = 0, 'x', '') FROM numbers(50000);
INSERT INTO t_ser_ext_stats2 SELECT number, if(number % 100 = 0, 'x', '') FROM numbers(50000);

SELECT 'before_merge', serialization_kind
FROM system.parts_columns
WHERE table = 't_ser_ext_stats2' AND database = currentDatabase() AND column = 's' AND active
ORDER BY name;

SYSTEM START MERGES t_ser_ext_stats2;
OPTIMIZE TABLE t_ser_ext_stats2 FINAL;

SELECT 'after_merge', serialization_kind
FROM system.parts_columns
WHERE table = 't_ser_ext_stats2' AND database = currentDatabase() AND column = 's' AND active
ORDER BY name;

-- The data is intact regardless of the serialization kind.
SELECT 'count', count() FROM t_ser_ext_stats2;
SELECT 'nonempty', count() FROM t_ser_ext_stats2 WHERE s != '';

DROP TABLE t_ser_ext_stats2;
