-- Multi-step PREWHERE must keep columns needed to evaluate `DEFAULT`s of columns missing in the part.
-- https://github.com/ClickHouse/ClickHouse/issues/111757

DROP TABLE IF EXISTS t_multistep_prewhere_default;

CREATE TABLE t_multistep_prewhere_default (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_multistep_prewhere_default SELECT number, toString(number % 7) FROM numbers(1000);
ALTER TABLE t_multistep_prewhere_default ADD COLUMN v Int64 DEFAULT k % 13;
ALTER TABLE t_multistep_prewhere_default ADD COLUMN w Int64 DEFAULT k + length(s);

SET optimize_move_to_prewhere = 1, move_all_conditions_to_prewhere = 1, enable_multiple_prewhere_read_steps = 1;

SELECT sum(v) FROM t_multistep_prewhere_default WHERE k <= 395 AND s = '3';
SELECT sum(w) FROM t_multistep_prewhere_default WHERE k <= 395 AND s = '3';
SELECT sum(v), sum(w) FROM t_multistep_prewhere_default WHERE k <= 395 AND s = '3' AND k % 2 = 1;
SELECT arraySort(groupUniqArray(v)) FROM (SELECT DISTINCT v FROM t_multistep_prewhere_default WHERE k <= 395 AND s = '3');
SELECT k, v FROM t_multistep_prewhere_default WHERE k <= 30 AND s = '3' ORDER BY k;

-- Parts written after the ALTER store v and w physically.
INSERT INTO t_multistep_prewhere_default SELECT number, toString(number % 7), 42, 43 FROM numbers(1000, 100);
SELECT sum(v), sum(w) FROM t_multistep_prewhere_default WHERE k <= 1050 AND s = '3';

DROP TABLE t_multistep_prewhere_default;
