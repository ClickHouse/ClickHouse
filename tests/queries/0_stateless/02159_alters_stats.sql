SET optimize_move_to_prewhere = 1;
SET allow_experimental_stats_for_prewhere_optimization = 1;
SET calculate_stats_during_insert = 1;
SET mutations_sync = 1;

DROP TABLE IF EXISTS alters_stats SYNC;

CREATE TABLE alters_stats
(
    a Int,
    b Int,
    c Int,
    d Int,
    k Int,
    heavy String,
    heavy2 String
)
ENGINE=MergeTree() ORDER BY a
SETTINGS experimantal_stats_update_period = 100000;

INSERT INTO alters_stats SELECT
    number AS a,       -- a > 10 -> ~100%
    number + 10 AS b,  -- b > 1000000 / 3 -> ~66%
    number + 20 AS c,  -- c > 1000000 / 2 -> ~50%
    number + 40 AS d,  -- d > 1000000 / 4 -> ~75%
    number + 1000 AS k,
    format('test {} test {}', toString(number), toString(number + 10)) AS heavy,
    format('text {} tafst{}afsd', toString(cityHash64(number)), toString(cityHash64(number))) AS heavy2
FROM system.numbers
LIMIT 1000000;

OPTIMIZE TABLE alters_stats FINAL;

SELECT 0;
SHOW CREATE TABLE alters_stats;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM alters_stats WHERE (a > 10) AND (b > 300000) AND (c > 500000) AND (d > 250000) AND 0=0;

ALTER TABLE alters_stats ADD STATISTIC st1 (a, b, c) TYPE AUTO;
ALTER TABLE alters_stats ADD STATISTIC st2 (d);
ALTER TABLE alters_stats MATERIALIZE STATISTIC st1;
SYSTEM RELOAD STATISTICS alters_stats;

SELECT 1;
SHOW CREATE TABLE alters_stats;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM alters_stats WHERE (a > 10) AND (b > 300000) AND (c > 500000) AND (d > 250000) AND 1=1;

ALTER TABLE alters_stats ADD STATISTIC st3 (d);
ALTER TABLE alters_stats DROP STATISTIC st2;
ALTER TABLE alters_stats CLEAR STATISTIC st1;
SYSTEM RELOAD STATISTICS alters_stats;

SELECT 2;
SHOW CREATE TABLE alters_stats;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM alters_stats WHERE (a > 10) AND (b > 300000) AND (c > 500000) AND (d > 250000) AND 2=2;


ALTER TABLE alters_stats DROP STATISTIC st1;
ALTER TABLE alters_stats MATERIALIZE STATISTIC st3;
SYSTEM RELOAD STATISTICS alters_stats;

SELECT 3;
SHOW CREATE TABLE alters_stats;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM alters_stats WHERE (a > 10) AND (b > 300000) AND (c > 500000) AND (d > 250000) AND 3=3;


DROP TABLE alters_stats SYNC;
