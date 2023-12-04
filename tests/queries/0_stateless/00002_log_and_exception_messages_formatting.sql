-- Tags: no-parallel, no-fasttest
-- no-parallel because we want to run this test when most of the other tests already passed

-- If this test fails, see the "Top patterns of log messages" diagnostics in the end of run.log

system flush logs;
drop table if exists logs;
create view logs as select * from system.text_log where now() - toIntervalMinute(120) < event_time;

-- Check that we don't have too many messages formatted with fmt::runtime or strings concatenation.
-- 0.001 threshold should be always enough, the value was about 0.00025
select 'runtime messages', max2(coalesce(sum(length(message_format_string) = 0) / countOrNull(), 0), 0.001) from logs;

-- Check the same for exceptions. The value was 0.03
select 'runtime exceptions', max2(coalesce(sum(length(message_format_string) = 0) / countOrNull(), 0), 0.05) from logs where message like '%DB::Exception%';

-- Check that we don't have too many short meaningless message patterns.
select 'messages shorter than 10', max2(countDistinctOrDefault(message_format_string), 10) from logs where length(message_format_string) < 10;

-- Same as above. Feel free to update the threshold or remove this query if really necessary
select 'messages shorter than 16', max2(countDistinctOrDefault(message_format_string), 40) from logs where length(message_format_string) < 16;

-- Same as above, but exceptions must be more informative. Feel free to update the threshold or remove this query if really necessary
select 'exceptions shorter than 30', max2(countDistinctOrDefault(message_format_string), 125) from logs where length(message_format_string) < 30 and message ilike '%DB::Exception%';


-- Avoid too noisy messages: top 1 message frequency must be less than 30%. We should reduce the threshold
select 'noisy messages', max2((select count() from logs group by message_format_string order by count() desc limit 1) / (select count() from logs), 0.30);

-- Same as above, but excluding Test level (actually finds top 1 Trace message)
with ('Access granted: {}{}', '{} -> {}') as frequent_in_tests
select 'noisy Trace messages', max2((select count() from logs where level!='Test' and message_format_string not in frequent_in_tests
    group by message_format_string order by count() desc limit 1) / (select count() from logs), 0.16);

-- Same as above for Debug
select 'noisy Debug messages', max2((select count() from logs where level <= 'Debug' group by message_format_string order by count() desc limit 1) / (select count() from logs), 0.09);

-- Same as above for Info
select 'noisy Info messages', max2((select count() from logs where level <= 'Information' group by message_format_string order by count() desc limit 1) / (select count() from logs), 0.05);

-- Same as above for Warning
with ('Not enabled four letter command {}') as frequent_in_tests
select 'noisy Warning messages', max2((select countOrDefault() from logs where level = 'Warning' and message_format_string not in frequent_in_tests
    group by message_format_string order by count() desc limit 1) / (select count() from logs), 0.01);

-- Same as above for Error
select 'noisy Error messages', max2((select countOrDefault() from logs where level = 'Error' group by message_format_string order by count() desc limit 1) / (select count() from logs), 0.02);

select 'no Fatal messages', count() from logs where level = 'Fatal';


-- Avoid too noisy messages: limit the number of messages with high frequency
select 'number of too noisy messages', max2(count(), 3) from (select count() / (select count() from logs) as freq, message_format_string from logs group by message_format_string having freq > 0.10);
select 'number of noisy messages', max2(count(), 10) from (select count() / (select count() from logs) as freq, message_format_string from logs group by message_format_string having freq > 0.05);

-- Each message matches its pattern (returns 0 rows)
-- FIXME maybe we should make it stricter ('Code:%Exception: '||s||'%'), but it's not easy because of addMessage
select 'incorrect patterns', max2(countDistinct(message_format_string), 15) from (
    select message_format_string, any(message) as any_message from logs
    where ((rand() % 8) = 0)
    and message not like (replaceRegexpAll(message_format_string, '{[:.0-9dfx]*}', '%') as s)
    and message not like (s || ' (skipped % similar messages)')
    and message not like ('%Exception: '||s||'%') group by message_format_string
) where any_message not like '%Poco::Exception%';

drop table logs;
