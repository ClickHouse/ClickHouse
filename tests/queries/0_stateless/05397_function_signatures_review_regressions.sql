-- `ifNotFinite` follows the common-type settings of `if`.
SELECT ifNotFinite(toFloat64('nan'), 'missing') AS x, toTypeName(x) SETTINGS use_variant_as_common_type = 1;
SELECT toTypeName(ifNotFinite(toInt64(1), toUInt64(2))) = toTypeName(if(isFinite(toInt64(1)), toInt64(1), toUInt64(2))) SETTINGS use_variant_as_common_type = 1;

-- `toInterval` accepts an interval argument.
SELECT toInterval(toIntervalDay(1), 'hour') AS x, toTypeName(x);

-- `arrayAvg` and `arrayProduct` accept `Enum` and `Interval` elements.
SELECT arrayAvg([CAST('a' AS Enum8('a' = 1))]), arrayProduct([toIntervalHour(2)]);
SELECT arrayAvg(x -> x, [CAST('a' AS Enum16('a' = 3))]), arrayProduct(x -> x, [toIntervalHour(3)]);

-- An `Interval` argument of a date-part extraction function takes no timezone and no extra arguments.
SELECT toHour(toIntervalHour(5));
SELECT toHour(toIntervalHour(5), 'UTC'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toHour(toIntervalHour(5), 'UTC', 123); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Documentation-only map signatures do not drive the result type.
SELECT toTypeName(mapConcat(map('a', 1::UInt8), map('b', 2::UInt64)));
SELECT toTypeName(mapEntries(map('a', 1::UInt8)));
