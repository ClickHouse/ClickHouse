SELECT cityHash64(toString(quantileDeterministicState(number, sipHash64(number)))) FROM numbers(8193);
SELECT quantileTDigest(0.8)(toDateTime('2106-02-07 09:28:15')); -- { serverError DECIMAL_OVERFLOW }
SELECT quantileTDigestWeighted(-0.)(toDateTime(10000000000.), 1); -- { serverError DECIMAL_OVERFLOW }
