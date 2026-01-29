-- Test that float literals work with Decimal types in if() function
-- https://github.com/ClickHouse/ClickHouse/issues/94671

-- Basic case: float literal 0. with Decimal
SELECT if(1, toDecimal64(1, 2), 0.);
SELECT if(0, toDecimal64(1, 2), 0.);

-- Float literal with different Decimal types
SELECT if(1, toDecimal32(42, 1), 0.);
SELECT if(1, toDecimal128(123, 3), 0.);

-- Float literals that can be exactly represented as Decimal
SELECT if(1, toDecimal64(1, 2), 1.5);
SELECT if(0, toDecimal64(1, 2), 1.5);

-- Multiple float literals
SELECT if(1, toDecimal64(10, 2), 0.) + if(0, toDecimal64(20, 2), 5.);

-- Nested if with Decimal and float
SELECT if(1, if(1, toDecimal64(1, 2), 0.), toDecimal64(2, 2));

-- Float literal that cannot be exactly represented should still fail
-- SELECT if(rand() < 0, toDecimal64(2, 1), 1/3); -- This should error (1/3 is not a constant)

-- Verify type is Decimal, not Float
SELECT toTypeName(if(1, toDecimal64(1, 2), 0.));

-- Integer literals should still work (existing behavior)
SELECT if(1, toDecimal64(1, 2), 0);
SELECT if(0, toDecimal64(1, 2), 42);
