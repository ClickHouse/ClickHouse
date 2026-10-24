set session_timezone='UTC';
-- toDateOrNull: pre-epoch and far future dates
select 'toDateOrNull:';
select '1960-01-01' as input, toDateOrNull('1960-01-01') as result;
select '1800-01-01' as input, toDateOrNull('1800-01-01') as result;
select '3000-01-01' as input, toDateOrNull('3000-01-01') as result;

-- toDateTimeOrNull: pre-epoch and far future datetimes
select 'toDateTimeOrNull:';
select '1960-01-01 00:00:00' as input, toDateTimeOrNull('1960-01-01 00:00:00') as result;
select '1800-01-01 00:00:00' as input, toDateTimeOrNull('1800-01-01 00:00:00') as result;
select '3000-01-01 00:00:00' as input, toDateTimeOrNull('3000-01-01 00:00:00') as result;

-- toDateTime64OrNull: pre-epoch and far future datetimes
select 'toDateTime64OrNull:';
select '1800-01-01 00:00:00' as input, toDateTime64OrNull('1800-01-01 00:00:00') as result;
select '3000-01-01 00:00:00' as input, toDateTime64OrNull('3000-01-01 00:00:00') as result;

-- accurateCastOrNull to Date
select 'accurateCastOrNull to Date:';
select '1960-01-01' as input, accurateCastOrNull('1960-01-01', 'Date') as result;
select '1800-01-01' as input, accurateCastOrNull('1800-01-01', 'Date') as result;
select '3000-01-01' as input, accurateCastOrNull('3000-01-01', 'Date') as result;

-- accurateCastOrNull to DateTime
select 'accurateCastOrNull to DateTime:';
select '1960-01-01' as input, accurateCastOrNull('1960-01-01', 'DateTime') as result;
select '1800-01-01' as input, accurateCastOrNull('1800-01-01', 'DateTime') as result;
select '3000-01-01' as input, accurateCastOrNull('3000-01-01', 'DateTime') as result;

-- accurateCastOrNull to DateTime with best_effort mode
select 'accurateCastOrNull to DateTime (best_effort):';
select '1960-01-01' as input, accurateCastOrNull('1960-01-01', 'DateTime') as result settings cast_string_to_date_time_mode='best_effort';
select '1800-01-01' as input, accurateCastOrNull('1800-01-01', 'DateTime') as result settings cast_string_to_date_time_mode='best_effort';
select '3000-01-01' as input, accurateCastOrNull('3000-01-01', 'DateTime') as result settings cast_string_to_date_time_mode='best_effort';

-- accurateCastOrNull to DateTime with best_effort_us mode
select 'accurateCastOrNull to DateTime (best_effort_us):';
select '1960-01-01' as input, accurateCastOrNull('1960-01-01', 'DateTime') as result settings cast_string_to_date_time_mode='best_effort_us';
select '1800-01-01' as input, accurateCastOrNull('1800-01-01', 'DateTime') as result settings cast_string_to_date_time_mode='best_effort_us';
select '3000-01-01' as input, accurateCastOrNull('3000-01-01', 'DateTime') as result settings cast_string_to_date_time_mode='best_effort_us';

-- accurateCastOrNull to DateTime64
select 'accurateCastOrNull to DateTime64:';
select '1800-01-01' as input, accurateCastOrNull('1800-01-01', 'DateTime64') as result;
select '3000-01-01' as input, accurateCastOrNull('3000-01-01', 'DateTime64') as result;

-- accurateCastOrNull to DateTime64 with best_effort mode
select 'accurateCastOrNull to DateTime64 (best_effort):';
select '1960-01-01' as input, accurateCastOrNull('1960-01-01', 'DateTime64') as result settings cast_string_to_date_time_mode='best_effort';
select '1800-01-01' as input, accurateCastOrNull('1800-01-01', 'DateTime64') as result settings cast_string_to_date_time_mode='best_effort';
select '3000-01-01' as input, accurateCastOrNull('3000-01-01', 'DateTime64') as result settings cast_string_to_date_time_mode='best_effort';

-- accurateCastOrNull to DateTime64 with best_effort_us mode
select 'accurateCastOrNull to DateTime64 (best_effort_us):';
select '1960-01-01' as input, accurateCastOrNull('1960-01-01', 'DateTime64') as result settings cast_string_to_date_time_mode='best_effort_us';
select '1800-01-01' as input, accurateCastOrNull('1800-01-01', 'DateTime64') as result settings cast_string_to_date_time_mode='best_effort_us';
select '3000-01-01' as input, accurateCastOrNull('3000-01-01', 'DateTime64') as result settings cast_string_to_date_time_mode='best_effort_us';

-- Test with CSV format input - West-of-UTC timezone (America/New_York, UTC-5)
select 'CSV input with America/New_York (default behavior):';
select * from format(CSV, 'dt DateTime', '1960-01-01 00:00:00') settings session_timezone='America/New_York';

select 'CSV input with America/New_York (timezone-aware saturation):';
select * from format(CSV, 'dt DateTime', '1960-01-01 00:00:00') settings session_timezone='America/New_York', date_time_overflow_behavior_use_local_timezone=1;

-- Test with CSV format input - East-of-UTC timezone (Europe/Kyiv, UTC+2/+3)
select 'CSV input with Europe/Kyiv (default behavior):';
select * from format(CSV, 'dt DateTime', '1960-01-01 00:00:00') settings session_timezone='Europe/Kyiv';

select 'CSV input with Europe/Kyiv (timezone-aware saturation):';
select * from format(CSV, 'dt DateTime', '1960-01-01 00:00:00') settings session_timezone='Europe/Kyiv', date_time_overflow_behavior_use_local_timezone=1;

-- Test with BestEffort mode
select 'CSV input with BestEffort mode - America/New_York (default behavior):';
select * from format(CSV, 'dt DateTime', '1960-01-01 00:00:00') settings session_timezone='America/New_York', date_time_input_format='best_effort';

select 'CSV input with BestEffort mode - America/New_York (timezone-aware saturation):';
select * from format(CSV, 'dt DateTime', '1960-01-01 00:00:00') settings session_timezone='America/New_York', date_time_input_format='best_effort', date_time_overflow_behavior_use_local_timezone=1;

select 'CSV input with BestEffort mode - Europe/Kyiv (default behavior):';
select * from format(CSV, 'dt DateTime', '1960-01-01 00:00:00') settings session_timezone='Europe/Kyiv', date_time_input_format='best_effort';

select 'CSV input with BestEffort mode - Europe/Kyiv (timezone-aware saturation):';
select * from format(CSV, 'dt DateTime', '1960-01-01 00:00:00') settings session_timezone='Europe/Kyiv', date_time_input_format='best_effort', date_time_overflow_behavior_use_local_timezone=1;

-- Test with BestEffortUS mode
select 'CSV input with BestEffortUS mode - America/New_York (default behavior):';
select * from format(CSV, 'dt DateTime', '01/01/1960 00:00:00') settings session_timezone='America/New_York', date_time_input_format='best_effort_us';

select 'CSV input with BestEffortUS mode - America/New_York (timezone-aware saturation):';
select * from format(CSV, 'dt DateTime', '01/01/1960 00:00:00') settings session_timezone='America/New_York', date_time_input_format='best_effort_us', date_time_overflow_behavior_use_local_timezone=1;

select 'CSV input with BestEffortUS mode - Europe/Kyiv (default behavior):';
select * from format(CSV, 'dt DateTime', '01/01/1960 00:00:00') settings session_timezone='Europe/Kyiv', date_time_input_format='best_effort_us';

select 'CSV input with BestEffortUS mode - Europe/Kyiv (timezone-aware saturation):';
select * from format(CSV, 'dt DateTime', '01/01/1960 00:00:00') settings session_timezone='Europe/Kyiv', date_time_input_format='best_effort_us', date_time_overflow_behavior_use_local_timezone=1;
