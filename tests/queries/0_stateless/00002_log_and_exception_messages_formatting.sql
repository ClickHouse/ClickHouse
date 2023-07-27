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

-- FIXME some of the following messages are not informative and it has to be fixed
create temporary table known_short_messages (s String) as select * from (select
['', '({}) Keys: {}', '({}) {}', 'Aggregating', 'Became leader', 'Cleaning queue', 'Creating set.',
'Cyclic aliases', 'Detaching {}', 'Executing {}', 'Fire events: {}', 'Found part {}', 'Loaded queue',
'No sharding key', 'No tables', 'Query: {}', 'Removed', 'Removed part {}', 'Removing parts.',
'Request URI: {}', 'Sending part {}', 'Sent handshake', 'Starting {}', 'Will mimic {}', 'Writing to {}',
'dropIfEmpty', 'loadAll {}', '{} ({}:{})', '{} -> {}', '{} {}', '{}: {}', 'Query was cancelled',
'Table {} already exists.', '{}%', 'Cancelled merging parts', 'All replicas are lost',
'Cancelled mutating parts', 'Read object: {}', 'New segment: {}', 'Unknown geometry type {}',
'Table {} is not replicated', '{} {}.{} already exists', 'Attempt to read after eof',
'Replica {} already exists', 'Convert overflow', 'key must be a tuple', 'Division by zero',
'No part {} in committed state', 'Files set to {}', 'Bytes set to {}', 'Sharding key {} is not used',
'Cannot parse datetime', 'Bad get: has {}, requested {}', 'There is no {} in {}', 'Numeric overflow',
'Polygon is not valid: {}', 'Decimal math overflow', '{} only accepts maps', 'Dictionary ({}) not found',
'Unknown format {}', 'Invalid IPv4 value', 'Invalid IPv6 value', 'Unknown setting {}',
'Unknown table function {}', 'Database {} already exists.', 'Table {} doesn''t exist',
'Invalid credentials', 'Part {} already exists', 'Invalid mode: {}', 'Log pulling is cancelled',
'JOIN {} cannot get JOIN keys', 'Unknown function {}{}', 'Cannot parse IPv6 {}',
'Not found address of host: {}', '{} must contain a tuple', 'Unknown codec family: {}',
'Expected const String column', 'Invalid partition format: {}', 'Cannot parse IPv4 {}',
'AST is too deep. Maximum: {}', 'Array sizes are too large: {}', 'Unable to connect to HDFS: {}',
'Shutdown is called for table', 'File is not inside {}',
'Table {} doesn''t exist', 'Database {} doesn''t exist', 'Table {}.{} doesn''t exist',
'File {} doesn''t exist', 'No such attribute ''{}''', 'User name ''{}'' is reserved',
'Could not find table: {}', 'Detached part "{}" not found', 'Unknown data type family: {}',
'Unknown input format {}', 'Cannot UPDATE key column {}', 'Substitution {} is not set',
'Cannot OPTIMIZE table: {}', 'User name is empty', 'Table name is empty', 'AST is too big. Maximum: {}',
'Unsupported cipher mode', 'Unknown explain kind ''{}''', 'Table {} was suddenly removed',
'No cache found by path: {}', 'No such column {} in table {}', 'There is no port named {}',
'Function {} cannot resize {}', 'Function {} is not parametric', 'Unknown key attribute ''{}''',
'Transaction was cancelled', 'Unknown parent id {}', 'Session {} not found', 'Mutation {} was killed',
'Table {}.{} doesn''t exist.', 'Table is not initialized yet', '{} is not an identifier',
'Column ''{}'' already exists', 'No macro {} in config', 'Invalid origin H3 index: {}',
'Invalid session timeout: ''{}''', 'Tuple cannot be empty', 'Database name is empty',
'Table {} is not a Dictionary', 'Expected function, got: {}', 'Unknown identifier: ''{}''',
'Failed to {} input ''{}''', '{}.{} is not a VIEW', 'Cannot convert NULL to {}', 'Dictionary {} doesn''t exist'
] as arr) array join arr;

-- Check that we don't have too many short meaningless message patterns.
select 'messages shorter than 10', max2(countDistinctOrDefault(message_format_string), 1) from logs where length(message_format_string) < 10 and message_format_string not in known_short_messages;

-- Same as above. Feel free to update the threshold or remove this query if really necessary
select 'messages shorter than 16', max2(countDistinctOrDefault(message_format_string), 3) from logs where length(message_format_string) < 16 and message_format_string not in known_short_messages;

-- Same as above, but exceptions must be more informative. Feel free to update the threshold or remove this query if really necessary
select 'exceptions shorter than 30', max2(countDistinctOrDefault(message_format_string), 30) from logs where length(message_format_string) < 30 and message ilike '%DB::Exception%' and message_format_string not in known_short_messages;


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
