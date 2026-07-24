-- Tags: replica, long

-- Regression test for possible CHECKSUM_DOESNT_MATCH due to per-column TTL bug.
-- That had been fixed in https://github.com/ClickHouse/ClickHouse/pull/35820

drop table if exists ttl_02265;
drop table if exists ttl_02265_r2;

-- The bug is appears only for Wide part.
create table ttl_02265    (date Date, key Int, value String TTL date + interval 1 month) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/ttl_02265', 'r1') order by key partition by date settings min_bytes_for_wide_part=0, min_bytes_for_full_part_storage=0;
create table ttl_02265_r2 (date Date, key Int, value String TTL date + interval 1 month) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/ttl_02265', 'r2') order by key partition by date settings min_bytes_for_wide_part=0, min_bytes_for_full_part_storage=0;

-- after, 20100101_0_0_0 will have ttl.txt and value.bin
insert into ttl_02265 values ('2010-01-01', 2010, 'foo');
-- after, 20100101_0_0_1 will not have neither ttl.txt nor value.bin
optimize table ttl_02265 final;
-- after, 20100101_0_0_2 will not have ttl.txt, but will have value.bin
optimize table ttl_02265 final;
system sync replica ttl_02265 STRICT;
system sync replica ttl_02265_r2 STRICT;

-- after detach/attach it will not have TTL in-memory, and will not have ttl.txt
detach table ttl_02265;
attach table ttl_02265;

-- So now the state for 20100101_0_0_2 is as follow:
--
--     table        | in_memory_ttl | ttl.txt | value.bin/mrk2
--     ttl_02265    | N             | N       | N
--     ttl_02265_r2 | Y             | N       | N
--
-- And hence on the replica that does not have TTL in-memory (this replica),
-- it will try to apply TTL, and the column will be dropped,
-- but on another replica the column won't be dropped since it has in-memory TTL and will not apply TTL.
-- and eventually this will lead to the following error:
--
--     MergeFromLogEntryTask: Code: 40. DB::Exception: Part 20100101_0_0_3 from r2 has different columns hash. (CHECKSUM_DOESNT_MATCH) (version 22.4.1.1). Data after merge is not byte-identical to data on another replicas. There could be several reasons: 1. Using newer version of compression library after server update. 2. Using another compression method. 3. Non-deterministic compression algorithm (highly unlikely). 4. Non-deterministic merge algorithm due to logical error in code. 5. Data corruption in memory due to bug in code. 6. Data corruption in memory due to hardware issue. 7. Manual modification of source data after server startup. 8. Manual modification of checksums stored in ZooKeeper. 9. Part format related settings like 'enable_mixed_granularity_parts' are different on different replicas. We will download merged part from replica to force byte-identical result.
--
optimize table ttl_02265 final;
system flush logs part_log;
select * from system.part_log where database = currentDatabase() and table like 'ttl_02265%' and error != 0 and errorCodeToName(error) != 'NO_REPLICA_HAS_PART';
