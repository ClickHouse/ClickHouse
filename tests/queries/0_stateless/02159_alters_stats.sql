DROP TABLE IF EXISTS prewhere SYNC;

SET optimize_move_to_prewhere = 1;
SET allow_experimental_stats_for_prewhere_optimization = 1;

-- SIMPLE tdigest stat
CREATE TABLE alters_stats
(
    a Int,
    b Int64,
    c Int,
    d FLOAT,
    k Int,
    heavy String,
    heavy2 String,
    STAT st1 (a, b, c) TYPE tdigest,
    STAT st2 (d)
)
ENGINE=MergeTree() ORDER BY a
SETTINGS experimantal_stats_update_period = 500;

INSERT INTO alters_stats SELECT
    number AS a,
    number + 10 AS b,
    number % 10 AS c,
    number + 10 AS d,
    number + 100 AS k,
    format('test {} test {}', toString(number), toString(number + 10)) AS heavy,
    format('text {} tafst{}afsd', toString(cityHash64(number)), toString(cityHash64(number))) AS heavy2
FROM system.numbers
LIMIT 1000000;

SHOW CREATE TABLE alters_stats;

ALTER TABLE alters_stats ADD STATISTIC st3 (d, k) TYPE tdigest;
ALTER TABLE alters_stats DROP STATISTIC st2;

SHOW CREATE TABLE alters_stats;

ALTER TABLE alters_stats MODIFY STATISTIC st3 (d);

SHOW CREATE TABLE alters_stats;

EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM alters_stats WHERE a == 10 AND b == 100 AND c == 0 AND d == 100;

ALTER TABLE alters_stats MATERIALIZE STATISTIC st1;
ALTER TABLE alters_stats MATERIALIZE STATISTIC st3;

SELECT sleep(1);

EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM alters_stats WHERE a == 10 AND b == 100 AND c == 0 AND d == 100;


DROP TABLE alters_stats SYNC;
