DROP TABLE IF EXISTS t_c3oollc8r;
CREATE TABLE t_c3oollc8r (c_k37 Int32, c_y String, c_bou Int32, c_g1 Int32, c_lfntfzg Int32, c_kntw50q Int32) ENGINE = MergeTree ORDER BY ();

SELECT (
    SELECT c_k37
    FROM t_c3oollc8r
    ) > c_lfntfzg
FROM remote('127.0.0.{1,2}', currentDatabase(), t_c3oollc8r);

DROP TABLE t_c3oollc8r;
