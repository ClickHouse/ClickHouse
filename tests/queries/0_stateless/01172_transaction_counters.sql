drop table if exists txn_counters;

create table txn_counters (n Int64, mintid DEFAULT transactionID()) engine=MergeTree order by n;

insert into txn_counters(n) values (1);
select transactionID();

-- stop background cleanup
system stop merges txn_counters;

begin transaction;
insert into txn_counters(n) values (2);
select 1, system.parts.name, txn_counters.mintid = system.parts.mintid from txn_counters join system.parts on txn_counters._part = system.parts.name where database=currentDatabase() and table='txn_counters' order by system.parts.name;
select 2, name, mincsn, maxtid, maxcsn from system.parts where database=currentDatabase() and table='txn_counters' order by system.parts.name;
rollback;

begin transaction;
insert into txn_counters(n) values (3);
select 3, system.parts.name, txn_counters.mintid = system.parts.mintid from txn_counters join system.parts on txn_counters._part = system.parts.name where database=currentDatabase() and table='txn_counters' order by system.parts.name;
select 4, name, mincsn, maxtid, maxcsn from system.parts where database=currentDatabase() and table='txn_counters' order by system.parts.name;
commit;

drop table txn_counters;
