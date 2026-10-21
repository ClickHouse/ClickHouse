-- A `DateTime` or lower-scale `DateTime64` constant compared with a `DateTime64` expression is
-- widened to the expression's scale, so `optimize_and_compare_chain` derives conditions through it.

SET enable_analyzer = 1;

SELECT 'datetime constant',
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(6, \'UTC\'), b DateTime64(6, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
        WHERE a < b AND b < toDateTime('2020-01-01 00:00:02', 'UTC')
        SETTINGS optimize_and_compare_chain = 1) WHERE explain LIKE '%function_name: less,%')
    >
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(6, \'UTC\'), b DateTime64(6, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
        WHERE a < b AND b < toDateTime('2020-01-01 00:00:02', 'UTC')
        SETTINGS optimize_and_compare_chain = 0) WHERE explain LIKE '%function_name: less,%');

-- The constant is widened to one instant: a time zone of its own does not shift it.
-- The counts read an indexed table, so a wrong derived bound prunes the row.
DROP TABLE IF EXISTS t_tz;
CREATE TABLE t_tz (a DateTime64(6, 'UTC'), b DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_tz VALUES ('2020-01-01 10:00:00.5', '2020-01-01 10:00:01');

SELECT 'datetime constant in another time zone',
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(6, \'UTC\'), b DateTime64(6, \'UTC\')', ('2020-01-01 10:00:00.5', '2020-01-01 10:00:01'))
        WHERE a < b AND b < toDateTime('2020-01-01 05:00:02', 'America/New_York')
        SETTINGS optimize_and_compare_chain = 1) WHERE explain LIKE '%function_name: less,%')
    >
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(6, \'UTC\'), b DateTime64(6, \'UTC\')', ('2020-01-01 10:00:00.5', '2020-01-01 10:00:01'))
        WHERE a < b AND b < toDateTime('2020-01-01 05:00:02', 'America/New_York')
        SETTINGS optimize_and_compare_chain = 0) WHERE explain LIKE '%function_name: less,%'),
    (SELECT count() FROM t_tz
        WHERE a < b AND b < toDateTime('2020-01-01 05:00:02', 'America/New_York')
        SETTINGS optimize_and_compare_chain = 1);

DROP TABLE t_tz;

DROP TABLE IF EXISTS t_scale;
CREATE TABLE t_scale (a DateTime64(6, 'UTC'), b DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_scale VALUES ('2020-01-01 00:00:02.122998', '2020-01-01 00:00:02.122999');

SELECT 'lower scale datetime64 constant',
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(6, \'UTC\'), b DateTime64(6, \'UTC\')', ('2020-01-01 00:00:02.122998', '2020-01-01 00:00:02.122999'))
        WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02.123', 3, 'UTC')
        SETTINGS optimize_and_compare_chain = 1) WHERE explain LIKE '%function_name: less,%')
    >
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(6, \'UTC\'), b DateTime64(6, \'UTC\')', ('2020-01-01 00:00:02.122998', '2020-01-01 00:00:02.122999'))
        WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02.123', 3, 'UTC')
        SETTINGS optimize_and_compare_chain = 0) WHERE explain LIKE '%function_name: less,%'),
    (SELECT count() FROM t_scale
        WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02.123', 3, 'UTC')
        SETTINGS optimize_and_compare_chain = 1);

DROP TABLE t_scale;

-- A constant of a higher scale would have to be narrowed, and one that overflows the expression's
-- scale cannot be widened: neither joins the chain.
SELECT 'higher scale constant stays unchained',
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(3, \'UTC\'), b DateTime64(3, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
        WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02', 6, 'UTC')
        SETTINGS optimize_and_compare_chain = 1) WHERE explain LIKE '%function_name: less,%')
    =
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(3, \'UTC\'), b DateTime64(3, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
        WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02', 6, 'UTC')
        SETTINGS optimize_and_compare_chain = 0) WHERE explain LIKE '%function_name: less,%');

SELECT 'overflowing constant stays unchained',
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(9, \'UTC\'), b DateTime64(9, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
        WHERE a < b AND b < toDateTime64('2299-12-31 00:00:00', 0, 'UTC')
        SETTINGS optimize_and_compare_chain = 1) WHERE explain LIKE '%function_name: less,%')
    =
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(9, \'UTC\'), b DateTime64(9, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
        WHERE a < b AND b < toDateTime64('2299-12-31 00:00:00', 0, 'UTC')
        SETTINGS optimize_and_compare_chain = 0) WHERE explain LIKE '%function_name: less,%');

-- A derived condition already implied by an existing one is not added again.
SELECT 'implied condition is not duplicated',
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(6, \'UTC\'), b DateTime64(6, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
        WHERE a < b AND b < toDateTime('2020-01-01 00:00:02', 'UTC') AND a < toDateTime('2020-01-01 00:00:02', 'UTC')
        SETTINGS optimize_and_compare_chain = 1) WHERE explain LIKE '%function_name: less,%')
    =
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT * FROM values('a DateTime64(6, \'UTC\'), b DateTime64(6, \'UTC\')', ('2020-01-01 00:00:00', '2020-01-01 00:00:01'))
        WHERE a < b AND b < toDateTime('2020-01-01 00:00:02', 'UTC') AND a < toDateTime('2020-01-01 00:00:02', 'UTC')
        SETTINGS optimize_and_compare_chain = 0) WHERE explain LIKE '%function_name: less,%');

-- The derived condition carries a typed constant, which is sent to the shards as is.
DROP TABLE IF EXISTS t_chain;
CREATE TABLE t_chain (a DateTime64(6, 'UTC'), b DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_chain VALUES ('2020-01-01 00:00:02.122998', '2020-01-01 00:00:02.122999'), ('2020-01-01 00:00:02.123', '2020-01-01 00:00:02.124');

SELECT 'remote',
    (SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_chain)
        WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02.123', 3, 'UTC')
        SETTINGS optimize_and_compare_chain = 1),
    (SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_chain)
        WHERE a < b AND b < toDateTime64('2020-01-01 00:00:02.123', 3, 'UTC')
        SETTINGS optimize_and_compare_chain = 0);

DROP TABLE t_chain;

-- Across a join the derived condition prunes the other table by its own key.
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
CREATE TABLE t_left (id UInt64, l_time DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right (id UInt64, r_time DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY r_time;
INSERT INTO t_left SELECT number, toDateTime64('2020-01-01 00:00:00', 6, 'UTC') + toIntervalSecond(number) FROM numbers(1000);
INSERT INTO t_right SELECT number, toDateTime64('2020-01-01 00:00:00', 6, 'UTC') + toIntervalSecond(number) FROM numbers(1000);

SELECT 'join prunes the other table';
SELECT count() > 0 FROM (EXPLAIN indexes = 1
    SELECT count() FROM t_left AS l INNER JOIN t_right AS r ON l.id = r.id
    WHERE l.l_time < toDateTime('2020-01-01 00:01:40', 'UTC') AND r.r_time <= l.l_time
    SETTINGS optimize_and_compare_chain = 1)
WHERE explain LIKE '%Condition:%r_time in%';
SELECT count() FROM t_left AS l INNER JOIN t_right AS r ON l.id = r.id
    WHERE l.l_time < toDateTime('2020-01-01 00:01:40', 'UTC') AND r.r_time <= l.l_time
    SETTINGS optimize_and_compare_chain = 1;
SELECT count() FROM t_left AS l INNER JOIN t_right AS r ON l.id = r.id
    WHERE l.l_time < toDateTime('2020-01-01 00:01:40', 'UTC') AND r.r_time <= l.l_time
    SETTINGS optimize_and_compare_chain = 0;

DROP TABLE t_left;
DROP TABLE t_right;
