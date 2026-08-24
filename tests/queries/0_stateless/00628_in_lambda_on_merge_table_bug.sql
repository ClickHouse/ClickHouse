drop table if exists test_in_tuple_1;
drop table if exists test_in_tuple_2;
drop table if exists test_in_tuple;

create table test_in_tuple_1 (key Int32, key_2 Int32, x Array(Int32), y Array(Int32)) engine = MergeTree order by (key, key_2);
create table test_in_tuple_2 (key Int32, key_2 Int32, x Array(Int32), y Array(Int32)) engine = MergeTree order by (key, key_2);
create table test_in_tuple as test_in_tuple_1 engine = Merge(currentDatabase(), '^test_in_tuple_[0-9]+$');

insert into test_in_tuple_1 values (1, 1, [1, 2], [1, 2]);
insert into test_in_tuple_2 values (2, 1, [1, 2], [1, 2]);
select key, arr_x, arr_y, _table from test_in_tuple left array join x as arr_x, y as arr_y order by _table, arr_x, arr_y;
select '-';
select key, arr_x, arr_y, _table from test_in_tuple left array join x as arr_x, y as arr_y where (key_2, arr_x, arr_y) in (1, 1, 1) order by _table, arr_x, arr_y;
select '-';
select key, arr_x, arr_y, _table from test_in_tuple left array join arrayFilter((t, x_0, x_1) -> (key_2, x_0, x_1) in (1, 1, 1), x, x ,y) as arr_x, arrayFilter((t, x_0, x_1) -> (key_2, x_0, x_1) in (1, 1, 1), y, x ,y) as arr_y where (key_2, arr_x, arr_y) in (1, 1, 1) order by _table, arr_x, arr_y;

drop table if exists test_in_tuple_1;
drop table if exists test_in_tuple_2;
drop table if exists test_in_tuple;
