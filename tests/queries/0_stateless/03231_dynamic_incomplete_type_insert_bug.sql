SET allow_experimental_dynamic_type = 1;
SET allow_suspicious_types_in_order_by = 1;
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 Array(Dynamic)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t1 (c0) VALUES ([]);
INSERT INTO t1 (c0) VALUES ([[]]), (['had', 1]);
INSERT INTO t1 (c0) VALUES ([['saw']]);
INSERT INTO t1 (c0) VALUES ([]);
OPTIMIZE TABLE t1 final;
SELECT * FROM t1 ORDER BY ALL;
DROP TABLE t1;

