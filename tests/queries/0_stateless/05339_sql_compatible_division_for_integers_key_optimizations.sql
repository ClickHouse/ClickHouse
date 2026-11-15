SET sql_compatible_division_for_integers = 1;
SET enable_parallel_replicas = 0;

CREATE TABLE exact_division_unsigned_key (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO exact_division_unsigned_key SELECT number FROM numbers(100);
-- The predicate must use the primary key through the division's monotonicity metadata.
SELECT groupArray(x) FROM (SELECT x FROM exact_division_unsigned_key WHERE x / 10 = 3 ORDER BY x) SETTINGS force_primary_key = 1;
SELECT x / 10 AS q FROM exact_division_unsigned_key ORDER BY q DESC LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT x / 10 AS q FROM exact_division_unsigned_key ORDER BY q DESC LIMIT 3 SETTINGS optimize_read_in_order = 0;

CREATE TABLE exact_division_signed_key (x Int64) ENGINE = MergeTree ORDER BY x;
INSERT INTO exact_division_signed_key SELECT toInt64(number) - 50 FROM numbers(100);
SELECT groupArray(x) FROM (SELECT x FROM exact_division_signed_key WHERE x / 10 = -3 ORDER BY x) SETTINGS force_primary_key = 1;
SELECT groupArray(x) FROM (SELECT x FROM exact_division_signed_key WHERE x / toInt8(-10) = 3 ORDER BY x) SETTINGS force_primary_key = 1;
SELECT x / toInt8(-10) AS q FROM exact_division_signed_key ORDER BY q DESC LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT x / toInt8(-10) AS q FROM exact_division_signed_key ORDER BY q DESC LIMIT 3 SETTINGS optimize_read_in_order = 0;

DROP TABLE exact_division_unsigned_key;
DROP TABLE exact_division_signed_key;

-- Potentially throwing constant divisors must still support lazy execution.
SELECT if(x = toInt64('-9223372036854775808'), toInt64(0), x / toInt8(-1))
FROM values('x Int64', (-9223372036854775808), (7)) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(number = 0, number / toUInt8(0), 0) FROM numbers(1, 1) SETTINGS short_circuit_function_evaluation = 'enable';
