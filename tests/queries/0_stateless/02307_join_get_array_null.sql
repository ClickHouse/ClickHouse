drop table if exists id_val;

create table id_val(id Int32, val Array(Int32)) engine Join(ANY, LEFT, id) settings join_use_nulls = 1;
select joinGet(id_val, 'val', toInt32(number)) from numbers(1);

drop table id_val;
