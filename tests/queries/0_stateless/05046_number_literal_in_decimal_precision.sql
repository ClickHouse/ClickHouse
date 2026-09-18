-- A decimal-point literal on the right of `IN` is a set element compared against the left-hand side
-- type, so it is parsed from its original text into the exact `Decimal` type, like `=` does. Going
-- through `Float64` would round `1.123456789012345678` and `1.123456789012345679` to the same value
-- and match both rows. A parenthesised list of literals is one tuple literal, so the elements of a
-- multi-value set are resolved too.

DROP TABLE IF EXISTS t_in_decimal;
CREATE TABLE t_in_decimal (d Decimal128(18)) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_in_decimal VALUES ('1.123456789012345678'), ('1.123456789012345679');

SELECT 'analyzer';
SELECT toString(d) FROM t_in_decimal WHERE d IN (1.123456789012345679) ORDER BY d SETTINGS enable_analyzer = 1;
SELECT toString(d) FROM t_in_decimal WHERE d NOT IN (1.123456789012345679) ORDER BY d SETTINGS enable_analyzer = 1;
SELECT toString(d) FROM t_in_decimal WHERE d IN (1.123456789012345678, 3.5) ORDER BY d SETTINGS enable_analyzer = 1;
SELECT toString(d) FROM t_in_decimal WHERE d NOT IN (1.123456789012345678, 3.5) ORDER BY d SETTINGS enable_analyzer = 1;
SELECT toString(d) FROM t_in_decimal WHERE d IN (1.123456789012345678, 1.123456789012345679) ORDER BY d SETTINGS enable_analyzer = 1;
SELECT toString(d) FROM t_in_decimal WHERE d GLOBAL IN (1.123456789012345679) ORDER BY d SETTINGS enable_analyzer = 1;

SELECT 'old analyzer';
SELECT toString(d) FROM t_in_decimal WHERE d IN (1.123456789012345679) ORDER BY d SETTINGS enable_analyzer = 0;
SELECT toString(d) FROM t_in_decimal WHERE d NOT IN (1.123456789012345679) ORDER BY d SETTINGS enable_analyzer = 0;
SELECT toString(d) FROM t_in_decimal WHERE d IN (1.123456789012345678, 3.5) ORDER BY d SETTINGS enable_analyzer = 0;
SELECT toString(d) FROM t_in_decimal WHERE d NOT IN (1.123456789012345678, 3.5) ORDER BY d SETTINGS enable_analyzer = 0;
SELECT toString(d) FROM t_in_decimal WHERE d IN (1.123456789012345678, 1.123456789012345679) ORDER BY d SETTINGS enable_analyzer = 0;
SELECT toString(d) FROM t_in_decimal WHERE d GLOBAL IN (1.123456789012345679) ORDER BY d SETTINGS enable_analyzer = 0;

SELECT 'constant left-hand side';
SELECT CAST('1.123456789012345678', 'Decimal128(18)') IN (1.123456789012345679) SETTINGS enable_analyzer = 1;
SELECT CAST('1.123456789012345679', 'Decimal128(18)') IN (1.123456789012345679) SETTINGS enable_analyzer = 1;
SELECT CAST('1.123456789012345678', 'Decimal128(18)') IN (1.123456789012345679) SETTINGS enable_analyzer = 0;
SELECT CAST('1.123456789012345679', 'Decimal128(18)') IN (1.123456789012345679) SETTINGS enable_analyzer = 0;

SELECT 'exponent notation';
SELECT toString(d) FROM t_in_decimal WHERE d IN (1123456789012345679e-18) ORDER BY d SETTINGS enable_analyzer = 1;
SELECT toString(d) FROM t_in_decimal WHERE d IN (1123456789012345679e-18) ORDER BY d SETTINGS enable_analyzer = 0;

SELECT 'float column keeps Float64 semantics';
DROP TABLE IF EXISTS t_in_float;
CREATE TABLE t_in_float (f Float64) ENGINE = MergeTree ORDER BY f;
INSERT INTO t_in_float VALUES (1.5), (2.5);
SELECT f FROM t_in_float WHERE f IN (1.5) ORDER BY f SETTINGS enable_analyzer = 1;
SELECT f FROM t_in_float WHERE f IN (1.5) ORDER BY f SETTINGS enable_analyzer = 0;
SELECT f FROM t_in_float WHERE f IN (25e-1, 999) ORDER BY f SETTINGS enable_analyzer = 1;
SELECT f FROM t_in_float WHERE f IN (25e-1, 999) ORDER BY f SETTINGS enable_analyzer = 0;

DROP TABLE t_in_float;
DROP TABLE t_in_decimal;
