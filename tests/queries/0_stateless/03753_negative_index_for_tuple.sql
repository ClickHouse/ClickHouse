-- { echo }

select (1, 2).-1, (1, 2).-2;
select ().-1; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
select (1, 2, 3).-4; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
select (1, 2).-1000000000000000000000000000000000000000000; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tupleElement((1, 'hello'), -10, 2);

drop table if exists a1;
drop table if exists a2;

create table a1 (i int, hash_key int) partition by (i, hash_key);
create table a2 (i int, j int, hash_key int) partition by (i, j, hash_key);

insert into a1 values (1, 0);
insert into a2 values (3, 4, 1);

select * from (select _partition_value.-1 from a1 union all select _partition_value.-1 from a2) order by all;

drop table a1;
drop table a2;

set enable_analyzer = 1;
set optimize_functions_to_subcolumns = 1;

drop table if exists test;
create table test (tuple Tuple(b UInt32, c Int32)) engine=Memory;
explain syntax run_query_tree_passes=1 select tupleElement(tuple, -1) from test;
drop table test;
