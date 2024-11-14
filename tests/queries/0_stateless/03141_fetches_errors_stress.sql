-- Tags: no-parallel
-- Tag no-parallel -- due to failpoints

create table data_r1 (key Int, value String) engine=ReplicatedMergeTree('/tables/{database}/data', '{table}') order by tuple();
create table data_r2 (key Int, value String) engine=ReplicatedMergeTree('/tables/{database}/data', '{table}') order by tuple();

system enable failpoint replicated_sends_failpoint;
insert into data_r1 select number, randomPrintableASCII(100) from numbers(100_000) settings max_block_size=1000, min_insert_block_size_rows=1000;
system disable failpoint replicated_sends_failpoint;

system sync replica data_r2;

system flush logs;
SET max_rows_to_read = 0; -- system.text_log can be really big
select event_time_microseconds, logger_name, message from system.text_log where level = 'Error' and message like '%Malformed chunked encoding%' order by 1 format LineAsString;

-- { echoOn }
select table, errorCodeToName(error), count() from system.part_log where database = currentDatabase() and error > 0 and errorCodeToName(error) not in ('FAULT_INJECTED', 'NO_REPLICA_HAS_PART', 'ATTEMPT_TO_READ_AFTER_EOF') group by 1, 2 order by 1, 2;
select count() from data_r1;
select count() from data_r2;
