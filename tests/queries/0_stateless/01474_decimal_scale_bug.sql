SELECT toDecimal32(1, 2) * toDecimal32(1, 1) x, toTypeName(x);
SELECT toDecimal32(1, 1) * toDecimal32(1, 2) x, toTypeName(x);
SELECT toDecimal32(1, 3) * toDecimal64(1, 1) x, toTypeName(x);
SELECT toDecimal32(1, 1) * toDecimal64(1, 3) x, toTypeName(x);
SELECT toDecimal32(1, 2) * toDecimal128(1, 3) x, toTypeName(x);
SELECT toDecimal32(1, 3) * toDecimal128(1, 2) x, toTypeName(x);

SELECT toDecimal64(1, 2) * toDecimal32(1, 1) x, toTypeName(x);
SELECT toDecimal64(1, 1) * toDecimal32(1, 2) x, toTypeName(x);
SELECT toDecimal64(1, 3) * toDecimal64(1, 1) x, toTypeName(x);
SELECT toDecimal64(1, 1) * toDecimal64(1, 3) x, toTypeName(x);
SELECT toDecimal64(1, 2) * toDecimal128(1, 3) x, toTypeName(x);
SELECT toDecimal64(1, 3) * toDecimal128(1, 2) x, toTypeName(x);

SELECT toDecimal128(1, 2) * toDecimal32(1, 1) x, toTypeName(x);
SELECT toDecimal128(1, 1) * toDecimal32(1, 2) x, toTypeName(x);
SELECT toDecimal128(1, 3) * toDecimal64(1, 1) x, toTypeName(x);
SELECT toDecimal128(1, 1) * toDecimal64(1, 3) x, toTypeName(x);
SELECT toDecimal128(1, 2) * toDecimal128(1, 3) x, toTypeName(x);
SELECT toDecimal128(1, 3) * toDecimal128(1, 2) x, toTypeName(x);
