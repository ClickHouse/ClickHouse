-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

SET mutations_sync = 2;
SET allow_experimental_statistics = 1;
DROP TABLE IF EXISTS t_alter_auto_statistics;

CREATE TABLE t_alter_auto_statistics
(
    a UInt64,
    b UInt64 STATISTICS (minmax),
    c String
)
ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';

INSERT INTO t_alter_auto_statistics VALUES (1, 1, 'xxx');

SELECT 'no auto statistics';

SELECT column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE table = 't_alter_auto_statistics' AND database = currentDatabase() AND active = 1
ORDER BY name, column;

ALTER TABLE t_alter_auto_statistics MODIFY SETTING auto_statistics_types = 'minmax, uniq, tdigest';
ALTER TABLE t_alter_auto_statistics MATERIALIZE STATISTICS ALL;

SELECT 'materialized minmax, uniq, tdigest';

SELECT column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE table = 't_alter_auto_statistics' AND database = currentDatabase() AND active = 1
ORDER BY name, column;

ALTER TABLE t_alter_auto_statistics MODIFY SETTING auto_statistics_types = 'minmax, uniq, countmin';
INSERT INTO t_alter_auto_statistics VALUES (2, 2, 'yyy');

SELECT 'added minmax, uniq, countmin';

SELECT column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE table = 't_alter_auto_statistics' AND database = currentDatabase() AND active = 1
ORDER BY name, column;

ALTER TABLE t_alter_auto_statistics MATERIALIZE STATISTICS ALL;

SELECT 'materialized minmax, uniq, countmin';

SELECT column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE table = 't_alter_auto_statistics' AND database = currentDatabase() AND active = 1
ORDER BY name, column;

ALTER TABLE t_alter_auto_statistics CLEAR STATISTICS ALL;

SELECT 'cleared statistics';

SELECT column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE table = 't_alter_auto_statistics' AND database = currentDatabase() AND active = 1
ORDER BY name, column;

DROP TABLE IF EXISTS t_alter_auto_statistics;
