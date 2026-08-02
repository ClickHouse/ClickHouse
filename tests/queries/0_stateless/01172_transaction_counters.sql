-- Tags: no-ordinary-database, no-encrypted-storage

drop table if exists txn_counters;

create table txn_counters (n Int64, creation_tid DEFAULT transactionID()) engine=MergeTree order by n SETTINGS old_parts_lifetime=3600;

insert into txn_counters(n) values (1);
select transactionID();

-- stop background cleanup
system stop merges txn_counters;

set throw_on_unsupported_query_inside_transaction=0;

begin transaction;
insert into txn_counters(n) values (2);
select 1, system.parts.name, txn_counters.creation_tid = system.parts.creation_tid from txn_counters join system.parts on txn_counters._part = system.parts.name where database=currentDatabase() and table='txn_counters' order by system.parts.name;
select 2, name, creation_csn, removal_tid, removal_csn from system.parts where database=currentDatabase() and table='txn_counters' order by system.parts.name;
rollback;

begin transaction;
insert into txn_counters(n) values (3);
select 3, system.parts.name, txn_counters.creation_tid = system.parts.creation_tid from txn_counters join system.parts on txn_counters._part = system.parts.name where database=currentDatabase() and table='txn_counters' order by system.parts.name;
select 4, name, creation_csn, removal_tid, removal_csn from system.parts where database=currentDatabase() and table='txn_counters' order by system.parts.name;
select 5, transactionID().3 == serverUUID();
commit;

detach table txn_counters;
attach table txn_counters;

begin transaction;
insert into txn_counters(n) values (4);
select 6, system.parts.name, txn_counters.creation_tid = system.parts.creation_tid from txn_counters join system.parts on txn_counters._part = system.parts.name where database=currentDatabase() and table='txn_counters' order by system.parts.name;
select 7, name, removal_tid, removal_csn from system.parts where database=currentDatabase() and table='txn_counters' and active order by system.parts.name;
select 8, transactionID().3 == serverUUID();
commit;

begin transaction;
insert into txn_counters(n) values (5);
alter table txn_counters drop partition id 'all';
rollback;

system flush logs transactions_info_log;
select indexOf((select arraySort(groupUniqArray(tid)) from system.transactions_info_log where database=currentDatabase() and table='txn_counters'), tid),
       type,
       thread_id!=0,
       length(query_id)=length(queryID()) or type='Commit' and query_id='',  -- ignore fault injection after commit
       tid_hash!=0,
       csn=0,
       part
from system.transactions_info_log
where tid in (select tid from system.transactions_info_log where database=currentDatabase() and table='txn_counters' and not (tid.1=1 and tid.2=1))
or (database=currentDatabase() and table='txn_counters') order by event_time;

drop table txn_counters;
