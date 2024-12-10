select moduloOrNull(10, toNullable(materialize(100)));
select moduloOrNull(93, toNullable(materialize(93)));
select moduloOrNull(91, toNullable(materialize(93)));
select moduloOrNull(94, toNullable(materialize(93)));

select toTypeName(moduloOrNull(1, 0));
select moduloOrNull(1, 0);
select moduloOrNull(1, materialize(0));
select moduloOrNull(materialize(1), 0);
select moduloOrNull(materialize(1), materialize(0));

select moduloOrNull(1.1, toNullable(materialize(toUInt64(0))));
select moduloOrNull(materialize(1), toNullable(materialize(toUInt64(0))));
select moduloOrNull(toNullable(materialize(1)), toNullable(materialize(toUInt64(0))));
select moduloOrNull(toNullable(materialize(toFloat32(1))), toNullable(materialize(toInt64(0))));
select moduloOrNull(1.1, toNullable(materialize(toInt128(0))));
select moduloOrNull(toNullable(materialize(toFloat64(1))), toNullable(materialize(toInt128(0))));
select moduloOrNull(toNullable(materialize(toFloat64(1))), toNullable(materialize(toInt256(0))));
select moduloOrNull(1.0, toNullable(materialize(toInt256(0))));

SELECT moduloOrNull(toNullable(materialize(1)), toNullable(materialize(0)));
SELECT moduloOrNull(toNullable(materialize(toFloat32(1))), toNullable(materialize(0)));
SELECT moduloOrNull(toNullable(materialize(toFloat32(1))), materialize(0));
SELECT moduloOrNull(toNullable(materialize(toFloat32(1))), toNullable(0));

SELECT moduloOrNull(materialize(1), CAST(materialize(NULL), 'Nullable(Float32)'));

SELECT moduloOrNull(toDecimal32(16.2, 2), 0.0);
SELECT moduloOrNull(toDecimal32(16.2, 2), toDecimal32(0.0, 2));

SELECT moduloOrNull((16.2), 0.0);
SELECT moduloOrNull(materialize(16.2), 0.0);
SELECT moduloOrNull(16.2, materialize(0.0));
SELECT moduloOrNull(materialize(16.2), materialize(0.0));

SELECT tupleModuloOrNull((15, 10, 5), (0, 3, 2));
SELECT tupleModuloOrNull((15, 10, 5), (5, 3, 2));
SELECT tupleModuloByNumberOrNull((15, 10, 5), 0);
SELECT tupleModuloByNumberOrNull((15, 10, 5), 2);
