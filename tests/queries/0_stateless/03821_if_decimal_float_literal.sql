-- Test for issue #94671: if() and coalesce() with Decimal and float literal (e.g. 0.)
-- Float literals that are exactly representable as Decimal should be accepted.

-- Integer literal (already worked)
SELECT if(1, toDecimal64(1, 2), 0);
SELECT if(0, toDecimal64(1, 2), 0);

-- Float literal 0. (was NO_COMMON_TYPE, now accepted)
SELECT if(1, toDecimal64(1, 2), 0.);
SELECT if(0, toDecimal64(1, 2), 0.);

-- Float literal 1.
SELECT if(0, toDecimal64(1, 2), 1.);
SELECT if(1, toDecimal64(1, 2), 1.);

-- COALESCE with Nullable(Decimal) and float literal (from issue)
SELECT COALESCE(CAST(1.0 AS Nullable(Decimal(7, 2))), 0.);
SELECT COALESCE(CAST(NULL AS Nullable(Decimal(7, 2))), 0.);

-- COALESCE with non-Nullable Decimal and float literal
SELECT COALESCE(CAST(1.0 AS Decimal(7, 2)), 0.);

-- multiIf with float literal (exactly representable)
SELECT multiIf(1 = 0, toDecimal64(2, 2), 0.);
SELECT multiIf(1 = 1, toDecimal64(2, 2), 0.);

-- Float32 literal (exactly representable)
SELECT if(0, toDecimal32(1, 1), 0.0::Float32);

-- Float that is not exactly representable (1/3) must NOT be promoted; still NO_COMMON_TYPE
SELECT if(0, toDecimal64(2, 2), 1/3); -- { serverError NO_COMMON_TYPE }
