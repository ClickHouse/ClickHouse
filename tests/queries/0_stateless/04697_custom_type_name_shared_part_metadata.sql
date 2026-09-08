-- Each part persists its own column types, so a part written after a metadata-only type change must
-- not take the types of an older part: `IDataType::equals` treats `SimpleAggregateFunction(sum, UInt64)`
-- and `UInt64` as equal, but only the first writes that name into `columns.txt`.

drop table if exists t_custom_type_name;

create table t_custom_type_name (id UInt64, v UInt64) engine = MergeTree order by id
settings max_bytes_to_merge_at_max_space_in_pool = 1; -- both parts must stay separate and alive

insert into t_custom_type_name values (1, 10);

alter table t_custom_type_name modify column v SimpleAggregateFunction(sum, UInt64);

insert into t_custom_type_name values (2, 20);

select name, type from system.parts_columns
where database = currentDatabase() and table = 't_custom_type_name' and column = 'v' and active
order by name;

drop table t_custom_type_name;

-- The same when the custom name is nested inside another type, which `IDataType::equals` also ignores.
drop table if exists t_nested_custom_type_name;

create table t_nested_custom_type_name (id UInt64, v Nullable(UInt8)) engine = MergeTree order by id
settings max_bytes_to_merge_at_max_space_in_pool = 1;

insert into t_nested_custom_type_name values (1, 1);

alter table t_nested_custom_type_name modify column v Nullable(Bool);

insert into t_nested_custom_type_name values (2, 0);

select name, type from system.parts_columns
where database = currentDatabase() and table = 't_nested_custom_type_name' and column = 'v' and active
order by name;

drop table t_nested_custom_type_name;
