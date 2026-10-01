-- https://github.com/ClickHouse/ClickHouse/issues/118354
-- A `DateTime64` value strictly between -1 and 0 seconds lost its sign, because the whole part is zero.
-- Typed query parameters and the `timestamp` function always use the basic parser, whatever the
-- session settings are, so they reproduce the problem at default settings.
SET param_value = '-0.500000';
SELECT toUnixTimestamp64Micro({value:DateTime64(6, 'UTC')});
SET param_value = '-.500000';
SELECT toUnixTimestamp64Micro({value:DateTime64(6, 'UTC')});
SET param_value = '-0.000001';
SELECT toUnixTimestamp64Micro({value:DateTime64(6, 'UTC')});
SET param_value = '-1.500000';
SELECT toUnixTimestamp64Micro({value:DateTime64(6, 'UTC')});
SET param_value = '0.500000';
SELECT toUnixTimestamp64Micro({value:DateTime64(6, 'UTC')});

SELECT toUnixTimestamp64Micro(timestamp('-0.500000'));
SELECT toUnixTimestamp64Micro(timestamp('-.500000'));
SELECT toUnixTimestamp64Micro(timestamp('-0.000001'));
