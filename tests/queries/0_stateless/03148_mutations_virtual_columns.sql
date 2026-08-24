DROP TABLE IF EXISTS t_mut_virtuals;

CREATE TABLE t_mut_virtuals (id UInt64, s String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_mut_virtuals VALUES (1, 'a');
INSERT INTO t_mut_virtuals VALUES (2, 'b');

SET insert_keeper_fault_injection_probability = 0;
SET mutations_sync = 2;

ALTER TABLE t_mut_virtuals UPDATE s = _part WHERE 1;
ALTER TABLE t_mut_virtuals DELETE WHERE _part LIKE 'all_1_1_0%';

SELECT * FROM t_mut_virtuals ORDER BY id;

DROP TABLE t_mut_virtuals;
