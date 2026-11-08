-- Compact logical results must retain row positions and SQL three-valued logic.
SET max_threads = 1;
SET max_block_size = 7;
SET compile_expressions = 0;
DROP TABLE IF EXISTS short_circuit_mask;
CREATE TABLE short_circuit_mask (id UInt64, a Nullable(Int8), b Nullable(Int8), c Nullable(Int8)) ENGINE = Memory;
INSERT INTO short_circuit_mask SELECT number, arrayElement([NULL, -2, 0, 1, 9]::Array(Nullable(Int8)), number % 5 + 1), arrayElement([NULL, -2, 0, 1, 9]::Array(Nullable(Int8)), intDiv(number, 5) % 5 + 1), arrayElement([NULL, -2, 0, 1, 9]::Array(Nullable(Int8)), intDiv(number, 25) % 5 + 1) FROM numbers(125);

SELECT 'disable';
SET short_circuit_function_evaluation = 'disable';
SELECT groupArray(tuple(result)) FROM (SELECT and(a, abs(b), abs(c)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, abs(b), abs(c)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT and(a, or(abs(b), abs(c))) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, and(abs(b), abs(c))) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT and(a, abs(b), NULL) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, abs(b), NULL) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT if(and(a, abs(b)), concat(toString(c), 'then'), concat(toString(c), 'else')) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT multiIf(and(a, abs(b)), toString(c), or(a, abs(c)), toString(b), toString(a)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT tuple(and(a, abs(b)), or(c, abs(b)), abs(b)) AS result FROM short_circuit_mask ORDER BY id);

SELECT 'enable';
SET short_circuit_function_evaluation = 'enable';
SELECT groupArray(tuple(result)) FROM (SELECT and(a, abs(b), abs(c)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, abs(b), abs(c)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT and(a, or(abs(b), abs(c))) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, and(abs(b), abs(c))) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT and(a, abs(b), NULL) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, abs(b), NULL) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT if(and(a, abs(b)), concat(toString(c), 'then'), concat(toString(c), 'else')) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT multiIf(and(a, abs(b)), toString(c), or(a, abs(c)), toString(b), toString(a)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT tuple(and(a, abs(b)), or(c, abs(b)), abs(b)) AS result FROM short_circuit_mask ORDER BY id);

SELECT 'force_enable';
SET short_circuit_function_evaluation = 'force_enable';
SELECT groupArray(tuple(result)) FROM (SELECT and(a, abs(b), abs(c)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, abs(b), abs(c)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT and(a, or(abs(b), abs(c))) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, and(abs(b), abs(c))) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT and(a, abs(b), NULL) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(a, abs(b), NULL) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT if(and(a, abs(b)), concat(toString(c), 'then'), concat(toString(c), 'else')) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT multiIf(and(a, abs(b)), toString(c), or(a, abs(c)), toString(b), toString(a)) AS result FROM short_circuit_mask ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT tuple(and(a, abs(b)), or(c, abs(b)), abs(b)) AS result FROM short_circuit_mask ORDER BY id);

-- Mixed masks, constant results, all-NULL selected results and filter tail sizes.
SET max_block_size = 65536;
SET short_circuit_function_evaluation = 'force_enable';
SELECT groupArray(tuple(and(number % 3, ignore(number) + 1), or(number % 3, ignore(number)), and(number % 2, nullIf(number % 2, 1)), or(number % 2 = 0, nullIf(number % 2, 1)))) FROM numbers(0);
SELECT groupArray(tuple(and(number % 3, ignore(number) + 1), or(number % 3, ignore(number)), and(number % 2, nullIf(number % 2, 1)), or(number % 2 = 0, nullIf(number % 2, 1)))) FROM numbers(1);
SELECT groupArray(tuple(and(number % 3, ignore(number) + 1), or(number % 3, ignore(number)), and(number % 2, nullIf(number % 2, 1)), or(number % 2 = 0, nullIf(number % 2, 1)))) FROM numbers(7);
SELECT groupArray(tuple(and(number % 3, ignore(number) + 1), or(number % 3, ignore(number)), and(number % 2, nullIf(number % 2, 1)), or(number % 2 = 0, nullIf(number % 2, 1)))) FROM numbers(64);
SELECT groupArray(tuple(and(number % 3, ignore(number) + 1), or(number % 3, ignore(number)), and(number % 2, nullIf(number % 2, 1)), or(number % 2 = 0, nullIf(number % 2, 1)))) FROM numbers(65);
SELECT groupArray(tuple(and(number % 3, ignore(number) + 1), or(number % 3, ignore(number)), and(number % 2, nullIf(number % 2, 1)), or(number % 2 = 0, nullIf(number % 2, 1)))) FROM numbers(129);
SELECT groupArray(tuple(and(number % 3, if(number % 7 = 0, toFloat64('nan'), toFloat64(number % 2))), or(number % 3, if(number % 7 = 0, toFloat64('nan'), toFloat64(number % 2))))) FROM numbers(65);
-- Full and empty masks also preserve non-normalized numeric truth values.
SELECT groupArray(tuple(and(materialize(toUInt8(1)), toInt8(number % 3) - 1), or(materialize(toUInt8(0)), toInt8(number % 3) - 1))) FROM numbers(65);
SELECT groupArray(tuple(and(materialize(toUInt8(0)), intDiv(1, number % 1)), or(materialize(toUInt8(1)), intDiv(1, number % 1)))) FROM numbers(65);
-- A decisive false/true skips an otherwise throwing suffix.
SELECT groupArray(and(number % 2 != 0, intDiv(1, toInt64(number % 2)))) FROM numbers(20);
SELECT groupArray(or(number % 2 = 0, intDiv(1, toInt64(number % 2)))) FROM numbers(20);
-- NULL alone is not decisive: both suffixes must still execute and throw.
SELECT and(if(number = 0, NULL, toNullable(toUInt8(1))), intDiv(1, toInt64(number))) FROM numbers(2); -- { serverError ILLEGAL_DIVISION }
SELECT or(if(number = 0, NULL, toNullable(toUInt8(0))), intDiv(1, toInt64(number))) FROM numbers(2); -- { serverError ILLEGAL_DIVISION }
-- A later decisive value overrides an earlier NULL and skips the throwing suffix.
SELECT groupArray(tuple(and(if(number = 0, NULL, toNullable(toUInt8(1))), number != 0, intDiv(1, toInt64(number))))) FROM numbers(8);
SELECT groupArray(tuple(or(if(number = 0, NULL, toNullable(toUInt8(0))), number = 0, intDiv(1, toInt64(number))))) FROM numbers(8);
-- LowCardinality dictionaries and nullable selected values.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS short_circuit_mask_lc;
CREATE TABLE short_circuit_mask_lc (id UInt64, gate UInt8, value LowCardinality(Nullable(UInt8))) ENGINE = Memory;
INSERT INTO short_circuit_mask_lc SELECT number, number % 3, if(number % 5 = 0, NULL, toNullable(toUInt8(number % 2))) FROM numbers(65);
SET short_circuit_function_evaluation = 'disable';
SELECT groupArray(tuple(result)) FROM (SELECT and(gate, abs(value)) AS result FROM short_circuit_mask_lc ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(gate, abs(value)) AS result FROM short_circuit_mask_lc ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT if(gate, and(abs(value), id % 2), or(abs(value), id % 2)) AS result FROM short_circuit_mask_lc ORDER BY id);
SET short_circuit_function_evaluation = 'force_enable';
SELECT groupArray(tuple(result)) FROM (SELECT and(gate, abs(value)) AS result FROM short_circuit_mask_lc ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT or(gate, abs(value)) AS result FROM short_circuit_mask_lc ORDER BY id);
SELECT groupArray(tuple(result)) FROM (SELECT if(gate, and(abs(value), id % 2), or(abs(value), id % 2)) AS result FROM short_circuit_mask_lc ORDER BY id);
DROP TABLE short_circuit_mask_lc;
DROP TABLE short_circuit_mask;
