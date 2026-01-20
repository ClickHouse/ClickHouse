-- Tags: no-bugfix-check
-- Test case for issue #94671: if() should accept float literals with Decimal branches

-- Works with integer literal (baseline)
SELECT if(rand() > 0, toDecimal64(1, 2), 0);

-- Should now work with float literal (previously failed with NO_COMMON_TYPE)
SELECT if(rand() > 0, toDecimal64(1, 2), 0.);
SELECT if(rand() > 0, toDecimal64(1, 2), 1.0);
SELECT if(rand() > 0, toDecimal64(1, 2), 2.5);

-- Test with reversed argument order
SELECT if(rand() > 0, 0., toDecimal64(1, 2));
SELECT if(rand() > 0, 1.0, toDecimal64(1, 2));

-- Test with different Decimal types
SELECT if(rand() > 0, toDecimal32(1, 2), 0.);
SELECT if(rand() > 0, toDecimal128(1, 2), 0.);
SELECT if(rand() > 0, toDecimal256(1, 2), 0.);

-- Test with Float32
SELECT if(rand() > 0, toDecimal64(1, 2), toFloat32(0.));

-- Test with actual condition evaluation
SELECT if(1, toDecimal64(10.5, 2), 0.);
SELECT if(0, toDecimal64(10.5, 2), 5.5);

-- Test with NULL condition
SELECT if(NULL, toDecimal64(1, 2), 0.);
