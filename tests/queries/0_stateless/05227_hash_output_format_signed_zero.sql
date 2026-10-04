-- The `Hash` format is a fingerprint of the result as it is, so values that compare equal but differ in
-- binary representation, such as `-0.` and `0.`, have different fingerprints, unlike in hash tables,
-- where the two zeros are one key. The fingerprints themselves are pinned so that the format stays stable.

SELECT -0. FORMAT Hash;
SELECT 0. FORMAT Hash;
SELECT toFloat32(-0.) FORMAT Hash;
SELECT toFloat32(0.) FORMAT Hash;
SELECT [-0., 0.] FORMAT Hash;
SELECT [0., 0.] FORMAT Hash;
SELECT toNullable(-0.) FORMAT Hash;
SELECT toNullable(0.) FORMAT Hash;
SELECT tuple(-0.) FORMAT Hash;
SELECT tuple(0.) FORMAT Hash;
SELECT -0.::Dynamic FORMAT Hash;
SELECT 0.::Dynamic FORMAT Hash;
SELECT number FROM system.numbers LIMIT 20 FORMAT Hash;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 20 FORMAT Hash;
SELECT toFloat64(number) / 3, [toFloat64(number), -0.], map('k', number), toDecimal64(number, 3) FROM numbers(5) FORMAT Hash;
SELECT if(number = 3, -0., 0.) FROM numbers(6) FORMAT Hash;
SELECT if(number = 3, -0., 0.) FROM numbers(6) SETTINGS max_block_size = 1 FORMAT Hash;
SELECT if(number = 3, -0., 0.) FROM numbers(6) SETTINGS max_block_size = 4 FORMAT Hash;
SELECT 0. FROM numbers(6) FORMAT Hash;

-- A `LowCardinality` dictionary can hold a negative zero as well, when it is read from a format.
-- The result is formatted on the server: a client rebuilds the dictionary of a received `LowCardinality`
-- column, where a negative zero becomes the default value, so `FORMAT Hash` in the client cannot see it.
SET allow_suspicious_low_cardinality_types = 1;
SET engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05227.hash', 'Hash', 'x LowCardinality(Float64)') SELECT x FROM format(TSV, 'x LowCardinality(Float64)', '-0');
SELECT * FROM file(currentDatabase() || '_05227.hash', 'LineAsString');
INSERT INTO FUNCTION file(currentDatabase() || '_05227.hash', 'Hash', 'x LowCardinality(Float64)') SELECT x FROM format(TSV, 'x LowCardinality(Float64)', '0');
SELECT * FROM file(currentDatabase() || '_05227.hash', 'LineAsString');
INSERT INTO FUNCTION file(currentDatabase() || '_05227.hash', 'Hash', 'x LowCardinality(Nullable(Float64))') SELECT x FROM format(TSV, 'x LowCardinality(Nullable(Float64))', '-0');
SELECT * FROM file(currentDatabase() || '_05227.hash', 'LineAsString');
INSERT INTO FUNCTION file(currentDatabase() || '_05227.hash', 'Hash', 'x LowCardinality(Nullable(Float64))') SELECT x FROM format(TSV, 'x LowCardinality(Nullable(Float64))', '0');
SELECT * FROM file(currentDatabase() || '_05227.hash', 'LineAsString');
INSERT INTO FUNCTION file(currentDatabase() || '_05227.hash', 'Hash', 'x Array(LowCardinality(Float64))') SELECT x FROM format(TSV, 'x Array(LowCardinality(Float64))', '[-0,0]');
SELECT * FROM file(currentDatabase() || '_05227.hash', 'LineAsString');
INSERT INTO FUNCTION file(currentDatabase() || '_05227.hash', 'Hash', 'x Array(LowCardinality(Float64))') SELECT x FROM format(TSV, 'x Array(LowCardinality(Float64))', '[0,0]');
SELECT * FROM file(currentDatabase() || '_05227.hash', 'LineAsString');
