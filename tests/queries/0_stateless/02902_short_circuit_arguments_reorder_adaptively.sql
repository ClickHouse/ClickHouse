drop table if exists reorder_t1;
create table if not exists reorder_t1 (a UInt32, b UInt32) Engine=Memory;

insert into reorder_t1 select number as a, number % 10 as b from numbers(512);
insert into reorder_t1 select number as a, number % 10 as b from numbers(512);

set short_circuit_function_evaluation='force_enable';

select 'enable reorder arguments';
select '-----';
select * from reorder_t1 where a < 5 and b = 0 settings enable_adaptive_reorder_short_circuit_arguments = 1;
select '-----';
select * from reorder_t1 where b = 0 and a < 5 settings enable_adaptive_reorder_short_circuit_arguments = 1;
select '-----';
select * from reorder_t1 where a < 5 or b = 0 settings enable_adaptive_reorder_short_circuit_arguments = 1;
select * from reorder_t1 where b = 0 or a < 5 settings enable_adaptive_reorder_short_circuit_arguments = 1;
select '-----';
select 'disable reorder arguments';
select '-----';
select * from reorder_t1 where a < 5 and b = 0 settings enable_adaptive_reorder_short_circuit_arguments = 0;
select '-----';
select * from reorder_t1 where b = 0 and a < 5 settings enable_adaptive_reorder_short_circuit_arguments = 0;
select '-----';
select * from reorder_t1 where a < 5 or b = 0 settings enable_adaptive_reorder_short_circuit_arguments = 0;
select '-----';
select * from reorder_t1 where b = 0 or a < 5 settings enable_adaptive_reorder_short_circuit_arguments = 0;

drop table if exists reorder_t1;
