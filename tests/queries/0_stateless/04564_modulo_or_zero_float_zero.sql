-- Regression for `moduloOrZero` with floating-point zero divisors.
-- It must return zero instead of NaN for both signs of zero. The four expressions below
-- exercise the vector/vector, vector/constant, constant/vector, and constant/constant paths.

SELECT 'Floating zero divisors return zero and not NaN';
SELECT
    moduloOrZero(materialize(toFloat32(5)), materialize(toFloat32(0))) = toFloat32(0),
    isNaN(moduloOrZero(materialize(toFloat32(5)), materialize(toFloat32(0)))) = 0,
    moduloOrZero(materialize(toFloat32(5)), toFloat32(-0.0)) = toFloat32(0),
    isNaN(moduloOrZero(materialize(toFloat32(5)), toFloat32(-0.0))) = 0,
    moduloOrZero(toFloat64(5), materialize(toFloat64(0))) = toFloat64(0),
    isNaN(moduloOrZero(toFloat64(5), materialize(toFloat64(0)))) = 0,
    moduloOrZero(identity(toFloat64(5)), identity(toFloat64(-0.0))) = toFloat64(0),
    isNaN(moduloOrZero(identity(toFloat64(5)), identity(toFloat64(-0.0)))) = 0;

SELECT 'A regular floating-point remainder is unchanged';
SELECT moduloOrZero(toFloat64(5.5), toFloat64(2)) = toFloat64(1.5);
