-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: shared merge tree doesn't loose data parts

SET max_rows_to_read = 0; -- system.text_log can be really big

create table rmt1 (d DateTime, n int) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '1') order by n partition by toYYYYMMDD(d);
create table rmt2 (d DateTime, n int) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '2') order by n partition by toYYYYMMDD(d);

system stop replicated sends rmt1;
insert into rmt1 values (now(), arrayJoin([1, 2])); -- { error BAD_ARGUMENTS }
insert into rmt1(n) select * from system.numbers limit arrayJoin([1, 2]); -- { serverError BAD_ARGUMENTS, INVALID_LIMIT_EXPRESSION }
insert into rmt1 values (now(), rand());
drop table rmt1;

system sync replica rmt2;
select lost_part_count from system.replicas where database = currentDatabase() and table = 'rmt2';
drop table rmt2;
SYSTEM FLUSH LOGS text_log;
select count() from system.text_log where logger_name like '%' || currentDatabase() || '%' and message ilike '%table with non-zero lost_part_count equal to%';


create table rmt1 (d DateTime, n int) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '1') order by n partition by tuple();
create table rmt2 (d DateTime, n int) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '2') order by n partition by tuple();

system stop replicated sends rmt1;
insert into rmt1 values (now(), rand());
drop table rmt1;

system sync replica rmt2;
select lost_part_count from system.replicas where database = currentDatabase() and table = 'rmt2';
drop table rmt2;
SYSTEM FLUSH LOGS text_log;
select count() from system.text_log where logger_name like '%' || currentDatabase() || '%' and message ilike '%table with non-zero lost_part_count equal to%';


create table rmt1 (n UInt8, m Int32, d Date, t DateTime) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '1') order by n partition by (n, m, d, t);
create table rmt2 (n UInt8, m Int32, d Date, t DateTime) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '2') order by n partition by (n, m, d, t);

system stop replicated sends rmt1;
insert into rmt1 values (rand(), rand(), now(), now());
insert into rmt1 values (rand(), rand(), now(), now());
insert into rmt1 values (rand(), rand(), now(), now());
drop table rmt1;

system sync replica rmt2;
drop table rmt2;
