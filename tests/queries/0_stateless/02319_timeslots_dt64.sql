SELECT timeSlots(toDateTime64('2000-01-02 03:04:05.12', 2, 'UTC'), toDecimal64(10000, 0));
SELECT timeSlots(toDateTime64('2000-01-02 03:04:05.233', 3, 'UTC'), toDecimal64(10000.12, 2), toDecimal64(634.1, 1));
SELECT timeSlots(toDateTime64('2000-01-02 03:04:05.3456', 4, 'UTC'), toDecimal64(600, 0), toDecimal64(30, 0));

SELECT timeSlots(toDateTime64('2000-01-02 03:04:05.23', 2, 'UTC')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSlots(toDateTime64('2000-01-02 03:04:05.345', 3, 'UTC'), toDecimal64(62.3, 1), toDecimal64(12.34, 2), 'one more'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSlots(toDateTime64('2000-01-02 03:04:05.456', 3, 'UTC'), 'wrong argument'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSlots(toDateTime64('2000-01-02 03:04:05.123', 3, 'UTC'), toDecimal64(600, 0), 'wrong argument'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSlots(toDateTime64('2000-01-02 03:04:05.1232', 4, 'UTC'), toDecimal64(600, 0), toDecimal64(0, 0)); -- { serverError ILLEGAL_COLUMN }