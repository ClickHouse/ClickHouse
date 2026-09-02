SELECT quantileTDigestWeighted(-0.)(toDateTime(10000000000.), 1); -- { serverError DECIMAL_OVERFLOW }
