SET optimize_move_to_prewhere = 1;
SET allow_experimental_stats_for_prewhere_optimization = 1;
SET calculate_stats_during_insert = 1;

DROP TABLE IF EXISTS prewhere SYNC;

-- SIMPLE tdigest stat
CREATE TABLE prewhere
(
    a Int,
    b Int64,
    c UInt64,
    d FLOAT,
    heavy String,
    heavy2 String,
    STATISTIC st (a, b, c, d) TYPE tdigest
)
ENGINE=MergeTree() ORDER BY a
SETTINGS experimantal_stats_update_period = 100000;

SELECT 'empty';
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c = 0 AND d == 100;

INSERT INTO prewhere SELECT
    number AS a,
    number + 10 AS b,
    number % 10 AS c,
    number + 10 AS d,
    format('test {} test {}', toString(number), toString(number + 10)) AS heavy,
    format('text {} tafst{}afsd', toString(cityHash64(number)), toString(cityHash64(number))) AS heavy2
FROM system.numbers
LIMIT 1000000;

SYSTEM RELOAD STATISTICS prewhere;

SELECT 'tdigest test';
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c < 10 AND d == 100;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND c > 0 AND d < 100 AND b < 100;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND c > 0 AND d > 100 AND b > 100;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND c > 0 AND d > 100 AND b < 100;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND c > 0 AND d < 100 AND b > 100;


DROP TABLE prewhere SYNC;

-- the same with automatic stats
-- auto creates granule_tdigest instead of simple tdigest
-- results for this test stay the same
CREATE TABLE prewhere
(
    a Int,
    b Int64,
    c Int,
    d FLOAT,
    heavy String,
    heavy2 String,
    STATISTIC st (a, b, c, d)
)
ENGINE=MergeTree() ORDER BY a
SETTINGS experimantal_stats_update_period = 100000;

SELECT 'empty 2';
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c < 10 AND d == 100;

INSERT INTO prewhere SELECT
    number AS a,
    number + 10 AS b,
    number % 10 AS c,
    number + 10 AS d,
    format('test {} test {}', toString(number), toString(number + 10)) AS heavy,
    format('text {} tafst{}afsd', toString(cityHash64(number)), toString(cityHash64(number))) AS heavy2
FROM system.numbers
LIMIT 1000000; -- TODO: turn off compact parts instead???

SYSTEM RELOAD STATISTICS prewhere;

SELECT 'auto test';

-- test simple selectivity
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 1000 AND c == 5 AND d == 1000;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND d < 10000 AND b < 10000;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND d > 10000 AND b > 10000;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND d > 10000 AND b < 10000;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND d < 10000 AND b > 10000;
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE d < 10000 AND b > 10000;
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE d > 10000 AND b < 10000;

-- test 10000 < ? < 20000
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE d < 10000 AND b > 10000 AND b < 20000;
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE d > 10000 AND b < 10000 AND d < 20000;;

-- test tuple
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE c > 0 AND d < 10000 AND (b, d) > (10000, 10000);
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE c > 0 AND (b, d) > (10000, 10000);
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE c > 0 AND (10000, 10000) < (b, d);
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE c > 0 AND d > 10000 AND (b, d) < (10000, 10000);
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE c > 0 AND d > 10000 AND (b, d) = (10000, 10000);

SELECT 'segment test';

EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE b < 1000 AND c < 10 AND d < 100000 AND a > 100 AND d > 10000;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE b < 100000 AND c < 10 AND d < 105000 AND a > 100 AND d > 10000;


DROP TABLE prewhere SYNC;
