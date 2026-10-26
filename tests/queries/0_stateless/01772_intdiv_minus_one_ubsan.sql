-- The range includes 9223372036854775808, which `toInt64` wraps to the minimal `Int64`, and
-- `intDiv` of the minimal signed number by -1 is an `ILLEGAL_DIVISION` exception (the quotient
-- does not fit in `Int64`). It used to wrap silently in the vectorized by-constant path.
SELECT intDiv(toInt64(number), -1) FROM numbers(9223372036854775807, 10); -- { serverError ILLEGAL_DIVISION }
-- The rest of the range still exercises the by-constant negation loop without UB.
SELECT intDiv(toInt64(number), -1) FROM numbers(9223372036854775809, 8);
