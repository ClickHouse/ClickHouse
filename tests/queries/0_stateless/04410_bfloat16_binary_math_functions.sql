-- Tests that the binary math functions accept BFloat16 arguments.

-- The result type is always Float64.
SELECT toTypeName(max2(toBFloat16(1), toBFloat16(2)));
SELECT toTypeName(atan2(toBFloat16(1), toBFloat16(2)));

-- Exactly representable BFloat16 values give exact results.
SELECT max2(toBFloat16(3), toBFloat16(5));
SELECT min2(toBFloat16(3), toBFloat16(5));
SELECT pow(toBFloat16(2), toBFloat16(10));
SELECT hypot(toBFloat16(3), toBFloat16(4));

-- BFloat16 widens to Float32, so the result must match the explicit Float32 computation.
SELECT
    atan2(toBFloat16(1), toBFloat16(1)) = atan2(toFloat32(toBFloat16(1)), toFloat32(toBFloat16(1))),
    pow(toBFloat16(3), toBFloat16(7)) = pow(toFloat32(toBFloat16(3)), toFloat32(toBFloat16(7))),
    hypot(toBFloat16(5), toBFloat16(12)) = hypot(toFloat32(toBFloat16(5)), toFloat32(toBFloat16(12)));

-- Mixed argument types.
SELECT max2(toBFloat16(3), 5);
SELECT min2(7, toBFloat16(5));
SELECT pow(toBFloat16(2), 0.5::Float64);

-- Column (non-const) form.
SELECT max2(x, toBFloat16(2)), min2(x, toBFloat16(2))
FROM (SELECT toBFloat16(number) AS x FROM numbers(5))
ORDER BY x;

-- Special values.
SELECT isNaN(pow(toBFloat16(-1), toBFloat16(0.5)));
SELECT max2(toBFloat16(inf), toBFloat16(1)), min2(toBFloat16(-inf), toBFloat16(1));

-- pow / hypot / atan2 with two non-const BFloat16 columns (must match the Float32 path).
SELECT
    pow(a, b) = pow(toFloat32(a), toFloat32(b)),
    hypot(a, b) = hypot(toFloat32(a), toFloat32(b)),
    atan2(a, b) = atan2(toFloat32(a), toFloat32(b))
FROM
(
    SELECT toBFloat16(number + 1) AS a, toBFloat16(number + 2) AS b
    FROM numbers(4)
)
ORDER BY a, b;

-- Left const + right BFloat16 column path.
SELECT pow(toBFloat16(2), b)
FROM (SELECT toBFloat16(number) AS b FROM numbers(4))
ORDER BY b;

SELECT
    hypot(toBFloat16(3), b) = hypot(toFloat32(toBFloat16(3)), toFloat32(b)),
    atan2(toBFloat16(1), b) = atan2(toFloat32(toBFloat16(1)), toFloat32(b))
FROM (SELECT toBFloat16(number) AS b FROM numbers(4))
ORDER BY b;
