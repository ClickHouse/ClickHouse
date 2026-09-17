-- `divideDecimal` rescales the dividend to `scale_b + result_scale` and then divides it one digit
-- at a time. The loop that primes the running remainder used to stop one digit early, so a dividend
-- left with a single digit after rescaling was never loaded and the result was silently 0.

-- The dividend is a single digit as written.
SELECT divideDecimal(toDecimal256(5, 0), toDecimal256(1, 0), 0);
SELECT divideDecimal(toDecimal256(-9, 0), toDecimal256(3, 0));
SELECT divideDecimal(toDecimal32(0.5, 1), toDecimal32(1, 0));

-- Both signs of the divisor, and divisions that leave a remainder.
SELECT divideDecimal(toDecimal256(5, 0), toDecimal256(-1, 0), 0);
SELECT divideDecimal(toDecimal256(-5, 0), toDecimal256(-1, 0), 0);
SELECT divideDecimal(toDecimal256(9, 0), toDecimal256(2, 0), 0);
SELECT divideDecimal(toDecimal256(-9, 0), toDecimal256(2, 0), 0);
SELECT divideDecimal(toDecimal32(0.7, 1), toDecimal32(3, 0), 1);

-- The dividend is left with a single digit only after being rescaled down to `result_scale`, so it
-- does not look like a single digit to the caller.
SELECT divideDecimal(toDecimal32(1.5, 1), toDecimal32(1, 0), 0);
SELECT divideDecimal(toDecimal32(5.6789, 4), toDecimal32(2, 0), 0);

-- The quotient is zero. The priming loop now consumes the whole dividend before the division
-- decides this, instead of leaving the running remainder at zero and returning without looking at
-- any digit.
SELECT divideDecimal(toDecimal256(3, 0), toDecimal256(5, 0), 0);
SELECT divideDecimal(toDecimal256(-3, 0), toDecimal256(5, 0), 0);
SELECT divideDecimal(toDecimal256(5, 0), toDecimal256(1234, 0), 0);

-- A longer dividend with a single-digit quotient. The priming loop now consumes the last digit
-- itself, so the division emits one digit instead of a leading zero and a digit.
SELECT divideDecimal(toDecimal256(12, 0), toDecimal256(3, 0), 0);
SELECT divideDecimal(toDecimal256(99, 0), toDecimal256(11, 0), 0);
SELECT divideDecimal(toDecimal256(-99, 0), toDecimal256(11, 0), 0);

-- A multi-digit quotient, which the priming loop reaches the same way as before.
SELECT divideDecimal(toDecimal256(-12, 0), toDecimal32(2.1, 1), 10);
