SET date_time_overflow_behavior = 'throw';
SELECT kqlBinAt(toDateTime64(toDecimal64('-62167219200.0000000', 7), 7, 'UTC'), toIntervalNanosecond(200), toDateTime64(toDecimal64('-62167219199.9999999', 7), 7, 'UTC')); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SET date_time_overflow_behavior = 'saturate';
SELECT kqlBinAt(toDateTime64(toDecimal64('-62167219200.0000000', 7), 7, 'UTC'), toIntervalNanosecond(200), toDateTime64(toDecimal64('-62167219199.9999999', 7), 7, 'UTC'));
