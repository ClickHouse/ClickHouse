-- Tags: no-fasttest

SELECT geoToS2(toFloat64(toUInt64(-1)), toFloat64(toUInt64(-1))); -- { serverError BAD_ARGUMENTS }
