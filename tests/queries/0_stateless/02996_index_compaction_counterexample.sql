DROP TABLE IF EXISTS b;
create table b (x Int64, y String) engine MergeTree order by (x, y) settings index_granularity=2;
insert into b values (0, 'a'), (1, 'b'),   (1, 'c');
select count() from b where x = 1 and y = 'b';
detach table b;
attach table b;
select count() from b where x = 1 and y = 'b';
DROP TABLE b;
