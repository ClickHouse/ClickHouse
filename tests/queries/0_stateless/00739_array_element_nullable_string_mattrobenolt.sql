drop temporary table if exists wups;
create temporary table wups (a Array(Nullable(String)));
select count(), a[1] from wups group by a[1];
insert into wups (a) values(['foo']);
select count(), a[1] from wups group by a[1];
insert into wups (a) values([]);
select count(), a[1] from wups group by a[1] order by a[1];

drop temporary table wups;

create temporary table wups (a Array(Nullable(String)));
insert into wups (a) values([]);
select a[1] from wups;
