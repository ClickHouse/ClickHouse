DROP TABLE IF EXISTS prewhere SYNC;

SET optimize_move_to_prewhere = 1;
SET allow_experimental_stats_for_prewhere_optimization = 1;

-- SIMPLE tdigest stat
CREATE TABLE prewhere
(
    a Int,
    b Int64,
    c Int,
    d FLOAT,
    heavy String,
    heavy2 String,
    STAT st (a, b, c, d) TYPE tdigest
)
ENGINE=MergeTree() ORDER BY a
SETTINGS experimantal_stats_update_period = 500;

EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c == 0 AND d == 100;

INSERT INTO prewhere SELECT
    number AS a,
    number + 10 AS b,
    number % 10 AS c,
    number + 10 AS d,
    format('test {} test {}', toString(number), toString(number + 10)) AS heavy,
    format('text {} tafst{}afsd', toString(cityHash64(number)), toString(cityHash64(number))) AS heavy2
FROM system.numbers
LIMIT 1000000;

SELECT sleep(1);

EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c == 0 AND d == 100;
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
    STAT st (a, b, c, d)
)
ENGINE=MergeTree() ORDER BY a
SETTINGS experimantal_stats_update_period = 500;

EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c == 0 AND d == 100;

INSERT INTO prewhere SELECT
    number AS a,
    number + 10 AS b,
    number % 10 AS c,
    number + 10 AS d,
    format('test {} test {}', toString(number), toString(number + 10)) AS heavy,
    format('text {} tafst{}afsd', toString(cityHash64(number)), toString(cityHash64(number))) AS heavy2
FROM system.numbers
LIMIT 1000000;

SELECT sleep(1);

EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c == 0 AND d == 100;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND c > 0 AND d < 100 AND b < 100;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND c > 0 AND d > 100 AND b > 100;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND c > 0 AND d > 100 AND b < 100;
EXPLAIN SYNTAX SELECT a, b, c, d, heavy, heavy2 FROM prewhere WHERE a > 0 AND c > 0 AND d < 100 AND b > 100;
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE c > 0 AND d < 10000 AND b > 10000;
EXPLAIN SYNTAX SELECT b, d FROM prewhere WHERE c > 0 AND d > 10000 AND b < 10000;


DROP TABLE prewhere SYNC;
