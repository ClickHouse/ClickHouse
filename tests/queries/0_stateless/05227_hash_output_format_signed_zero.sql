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
