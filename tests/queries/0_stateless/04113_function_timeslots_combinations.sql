-- Exercise all timeSlots vector/constant combinations for DateTime and DateTime64
-- plus error paths for invalid slot size.

SELECT 'DateTime variants';

-- const start, const duration, default slot (compile-time constant folded)
SELECT timeSlots(toDateTime('2020-01-01 00:00:00', 'UTC'), 600::UInt32);

-- vector start, vector duration (vectorVector)
SELECT timeSlots(
    materialize(toDateTime('2020-01-01 00:00:00', 'UTC')) + number * 60,
    materialize(600::UInt32),
    300
) FROM numbers(3);

-- vector start, const duration (vectorConstant)
SELECT timeSlots(
    materialize(toDateTime('2020-01-01 00:00:00', 'UTC')) + number * 60,
    600::UInt32,
    300
) FROM numbers(3);

-- const start, vector duration (constantVector)
SELECT timeSlots(
    toDateTime('2020-01-01 00:00:00', 'UTC'),
    materialize((600 + number * 300)::UInt32),
    300
) FROM numbers(3);

SELECT 'DateTime64 variants';

-- vector start, vector duration (DateTime64 vectorVector) with mixed scales
SELECT timeSlots(
    materialize(toDateTime64('2020-01-01 00:00:00.123', 3, 'UTC') + toIntervalSecond(number * 60)),
    materialize(toDecimal64(600.1, 1)),
    toDecimal64(300, 0)
) FROM numbers(3);

-- vector start, const duration (DateTime64 vectorConstant)
SELECT timeSlots(
    materialize(toDateTime64('2020-01-01 00:00:00.5', 3, 'UTC') + toIntervalSecond(number * 60)),
    toDecimal64(600.1, 1),
    toDecimal64(300, 0)
) FROM numbers(3);

-- const start, vector duration (DateTime64 constantVector)
SELECT timeSlots(
    toDateTime64('2020-01-01 00:00:00.5', 3, 'UTC'),
    materialize((600 + number * 300)::Decimal64(1)),
    toDecimal64(300, 0)
) FROM numbers(3);

SELECT 'Scale selection: returned type picks max scale';

-- returned scale should be max(dt_scale, duration_scale)
SELECT toTypeName(timeSlots(toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), toDecimal64(600, 6)));
SELECT toTypeName(timeSlots(toDateTime64('2020-01-01 00:00:00', 6, 'UTC'), toDecimal64(600, 2)));

SELECT 'Error paths';

-- third argument (slot size) must be positive integer for DateTime
SELECT timeSlots(toDateTime('2020-01-01 00:00:00', 'UTC'), 600::UInt32, 0); -- { serverError ILLEGAL_COLUMN }
SELECT timeSlots(toDateTime('2020-01-01 00:00:00', 'UTC'), 600::UInt32, materialize(300::UInt32)); -- { serverError ILLEGAL_COLUMN }

-- third argument (slot size) must be positive Decimal64 for DateTime64
SELECT timeSlots(toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), toDecimal64(600, 0), toDecimal64(0, 0)); -- { serverError ILLEGAL_COLUMN }
SELECT timeSlots(toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), toDecimal64(600, 0), materialize(toDecimal64(300, 0))); -- { serverError ILLEGAL_COLUMN }

-- unsupported types for first argument
SELECT timeSlots('string', 600::UInt32); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSlots(toDate('2020-01-01'), 600::UInt32); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- wrong number of arguments
SELECT timeSlots(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSlots(toDateTime('2020-01-01 00:00:00', 'UTC')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSlots(toDateTime('2020-01-01 00:00:00', 'UTC'), 600::UInt32, 300, 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
