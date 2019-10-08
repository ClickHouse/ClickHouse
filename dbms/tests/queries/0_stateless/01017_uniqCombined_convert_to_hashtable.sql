select uniqCombined(number) from numbers(bitShiftLeft(toUInt64(1), 13));
select uniqCombined(number) from numbers(bitShiftLeft(toUInt64(1), 13)+1);
select uniqCombined(number) from numbers(bitShiftLeft(toUInt64(1), 14));
select uniqCombined(number) from numbers(bitShiftLeft(toUInt64(1), 14)+1);
-- String uses 64-bit hash
select 'String';
select uniqCombined(toString(number)) from numbers(bitShiftLeft(toUInt64(1), 13));
select uniqCombined(toString(number)) from numbers(bitShiftLeft(toUInt64(1), 13)+1);
