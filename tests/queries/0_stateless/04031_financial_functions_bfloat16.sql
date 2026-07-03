-- Test that BFloat16 arguments are rejected by financial functions (they only support Float32/Float64).
-- Previously this caused a LOGICAL_ERROR exception because the type validator accepted BFloat16
-- but the execution dispatch only handled Float32 and Float64.

SELECT financialNetPresentValue(toBFloat16(0.1), [-100., 110.]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT financialNetPresentValue(0.1, [toBFloat16(-100), toBFloat16(110)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT financialNetPresentValueExtended(toBFloat16(0.1), [-10000., 5750.], [toDate('2020-01-01'), toDate('2020-03-01')]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT financialNetPresentValueExtended(0.1, [toBFloat16(-10000), toBFloat16(5750)], [toDate('2020-01-01'), toDate('2020-03-01')]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT financialInternalRateOfReturn([toBFloat16(-100), toBFloat16(110)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT financialInternalRateOfReturn([-100., 110.], toBFloat16(0.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT financialInternalRateOfReturnExtended([toBFloat16(-10000), toBFloat16(5750)], [toDate('2020-01-01'), toDate('2020-03-01')]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT financialInternalRateOfReturnExtended([-10000., 5750.], [toDate('2020-01-01'), toDate('2020-03-01')], toBFloat16(0.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
