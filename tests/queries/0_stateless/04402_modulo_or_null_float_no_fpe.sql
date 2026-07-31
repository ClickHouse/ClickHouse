-- Regression for `moduloOrNull` / `positiveModuloOrNull` with floating-point operands.
-- The smallest positive Float32/Float64 (`numeric_limits<Float>::min()`, the smallest positive
-- value, not the most negative) divided by -1 must return the finite remainder, not NULL:
-- floating-point modulo never raises a floating-point exception, and the `INT_MIN / -1` overflow
-- only applies to integer modulo. Only division by zero maps to NULL (matching `divideOrNull`).
-- See https://github.com/ClickHouse/ClickHouse/pull/101976

SELECT 'Float32 smallest-positive % -1 is the finite remainder, not NULL';
SELECT moduloOrNull(toFloat32(1.17549435e-38), toFloat32(-1)) IS NULL;
SELECT moduloOrNull(toFloat32(1.17549435e-38), toFloat32(-1)) = toFloat32(1.17549435e-38);
SELECT positiveModuloOrNull(toFloat32(1.17549435e-38), toFloat32(-1)) IS NULL;
SELECT positiveModuloOrNull(toFloat32(1.17549435e-38), toFloat32(-1)) = toFloat32(1.17549435e-38);
SELECT moduloOrNull(materialize(toFloat32(1.17549435e-38)), materialize(toFloat32(-1))) IS NULL;
SELECT moduloOrNull(materialize(toFloat32(1.17549435e-38)), materialize(toFloat32(-1))) = toFloat32(1.17549435e-38);

SELECT 'Float64 smallest-positive % -1 is the finite remainder, not NULL';
SELECT moduloOrNull(toFloat64(2.2250738585072014e-308), toFloat64(-1)) IS NULL;
SELECT moduloOrNull(toFloat64(2.2250738585072014e-308), toFloat64(-1)) = toFloat64(2.2250738585072014e-308);
SELECT positiveModuloOrNull(toFloat64(2.2250738585072014e-308), toFloat64(-1)) IS NULL;
SELECT positiveModuloOrNull(toFloat64(2.2250738585072014e-308), toFloat64(-1)) = toFloat64(2.2250738585072014e-308);

SELECT 'Floating modulo by zero still maps to NULL';
SELECT moduloOrNull(toFloat32(1.5), toFloat32(0)) IS NULL;
SELECT positiveModuloOrNull(toFloat32(1.5), toFloat32(0)) IS NULL;
SELECT moduloOrNull(materialize(toFloat64(1.5)), materialize(toFloat64(0))) IS NULL;

SELECT 'A regular floating remainder is unaffected';
SELECT moduloOrNull(toFloat64(5.5), toFloat64(2)) = toFloat64(1.5);
SELECT moduloOrNull(toFloat32(5.5), toFloat32(-2)) = toFloat32(1.5);
