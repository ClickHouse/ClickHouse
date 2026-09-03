DROP TABLE IF EXISTS t_02559;
CREATE TABLE t_02559 (a Int64, b Int64, c Int64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_02559 SELECT number, number, number FROM numbers(3);

SET enable_multiple_prewhere_read_steps = 1;

-- { echoOn }

SELECT a FROM t_02559 PREWHERE sin(a) < b AND sin(a) < c;
SELECT sin(a) > 2 FROM t_02559 PREWHERE sin(a) < b AND sin(a) < c;
SELECT sin(a) < a FROM t_02559 PREWHERE sin(a) < b AND sin(a) < c AND sin(a) > -a;
SELECT sin(a) < a FROM t_02559 PREWHERE sin(a) < b AND a <= c AND sin(a) > -a;

-- {echoOff}

DROP TABLE t_02559;
