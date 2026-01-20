-- Test case for issue #94671: if() should accept float literals with Decimal branches
-- This is a bugfix test - on master, these queries throw NO_COMMON_TYPE error

-- Basic case: float literal 0. with Decimal64
SELECT if(rand() > 0, toDecimal64(1, 2), 0.);

-- Float literal 1.0 with Decimal64
SELECT if(rand() > 0, toDecimal64(1, 2), 1.0);

-- Float literal with Decimal32
SELECT if(1, toDecimal32(42, 4), 0.);

-- Float literal with Decimal128
SELECT if(1, toDecimal128(100, 6), 2.5);

-- Verify integer literals still work (regression test)
SELECT if(rand() > 0, toDecimal64(1, 2), 0);
