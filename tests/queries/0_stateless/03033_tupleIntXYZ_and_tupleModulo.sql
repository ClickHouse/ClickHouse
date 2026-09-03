SELECT tupleIntDiv((15, 10, 5), (0, 0, 0)); -- { serverError ILLEGAL_DIVISION }
SELECT tupleIntDiv((15, 10, 5), (5, 5, 5));
SELECT tupleIntDiv((15, 10, 5), (5.5, 5.5, 5.5));
SELECT tupleIntDivOrZero((5, 10, 15), (0, 0, 0)); -- no error thrown for zero divisors
SELECT tupleIntDivByNumber((15, 10, 5), 0); -- { serverError ILLEGAL_DIVISION }
SELECT tupleIntDivByNumber((15, 10, 5), 5);
SELECT tupleIntDivByNumber((15.2, 10.7, 5.5), 5.8);
SELECT tupleIntDivOrZeroByNumber((15, 10, 5), 5);
SELECT tupleIntDivOrZeroByNumber((15, 10, 5), 0); -- no error thrown for zero divisors
SELECT tupleModulo((15, 10, 5), (0, 3, 2)); -- { serverError ILLEGAL_DIVISION }
SELECT tupleModulo((15, 10, 5), (5, 3, 2));
SELECT tupleModuloByNumber((15, 10, 5), 0); -- { serverError ILLEGAL_DIVISION }
SELECT tupleModuloByNumber((15, 10, 5), 2);