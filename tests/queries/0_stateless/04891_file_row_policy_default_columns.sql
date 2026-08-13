-- https://github.com/ClickHouse/ClickHouse/issues/114616
-- Row policy / PREWHERE over File tables must compute DEFAULT columns from real
-- dependency columns, not prune those inputs away before AddingDefaultsTransform.

SET enable_json_type = 1;

INSERT INTO FUNCTION file('04891_rp.parquet', Parquet)
SELECT number AS k, number % 10 AS a, concat('val_', toString(number)) AS s
FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1;

-- Case 1: row policy on a DEFAULT column missing from the file.
DROP TABLE IF EXISTS t_rp_j;
CREATE TABLE t_rp_j
(
    k UInt64,
    a UInt64,
    s String,
    j JSON DEFAULT toJSONString(map('user', map('name', concat('u', toString(a)))))
)
ENGINE = File(Parquet, '04891_rp.parquet');

CREATE ROW POLICY pol_j ON t_rp_j USING j.user.name != 'u0' TO ALL;

SELECT count() FROM t_rp_j;
SELECT k, s FROM t_rp_j ORDER BY k LIMIT 3;

DROP ROW POLICY pol_j ON t_rp_j;
DROP TABLE t_rp_j;

-- Case 2: policy on a file column; SELECT a DEFAULT column with PREWHERE.
DROP TABLE IF EXISTS t_rp_d;
CREATE TABLE t_rp_d
(
    k UInt64,
    a UInt64,
    s String,
    d UInt64 DEFAULT a * 2
)
ENGINE = File(Parquet, '04891_rp.parquet');

CREATE ROW POLICY pol_d ON t_rp_d USING a != 0 TO ALL;

SELECT k, d FROM t_rp_d PREWHERE s != 'val_2' ORDER BY k LIMIT 3;
SELECT k, d FROM t_rp_d ORDER BY k LIMIT 3;

DROP ROW POLICY pol_d ON t_rp_d;
DROP TABLE t_rp_d;
