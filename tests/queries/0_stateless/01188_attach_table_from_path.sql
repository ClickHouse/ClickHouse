-- Tags: no-replicated-database, memory-engine

drop table if exists test;
drop table if exists file;
drop table if exists mt;

attach table test from 'some/path' (n UInt8) engine=Memory; -- { serverError NOT_IMPLEMENTED }
attach table test from '/etc/passwd' (s String) engine=File(TSVRaw); -- { serverError PATH_ACCESS_DENIED }
attach table test from '../../../../../../../../../etc/passwd' (s String) engine=File(TSVRaw); -- { serverError PATH_ACCESS_DENIED }
attach table test from 42 (s String) engine=File(TSVRaw); -- { clientError SYNTAX_ERROR }

insert into table function file('01188_attach/file/data.TSV', 'TSV', 's String, n UInt8') values ('file', 42);
attach table file from '01188_attach/file' (s String, n UInt8) engine=File(TSV);
select * from file;
detach table file;
attach table file;
select * from file;

attach table mt from '01188_attach/file' (n UInt8, s String) engine=MergeTree order by n;
select * from mt;
insert into mt values (42, 'mt');
select * from mt;
detach table mt;
attach table mt;
select * from mt;

drop table file;
drop table mt;
