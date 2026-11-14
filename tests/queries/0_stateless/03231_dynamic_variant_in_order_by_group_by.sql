set allow_experimental_variant_type=1;
set allow_experimental_dynamic_type=1;

drop table if exists test;

create table test (d Dynamic) engine=MergeTree order by d; -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Dynamic) engine=MergeTree order by tuple(d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Dynamic) engine=MergeTree order by array(d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Dynamic) engine=MergeTree order by map('str', d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Dynamic) engine=MergeTree order by tuple() primary key d; -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Dynamic) engine=MergeTree order by tuple() partition by d; -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Dynamic) engine=MergeTree order by tuple() partition by tuple(d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Dynamic) engine=MergeTree order by tuple() partition by array(d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Dynamic) engine=MergeTree order by tuple() partition by map('str', d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}

create table test (d Variant(UInt64)) engine=MergeTree order by d; -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Variant(UInt64)) engine=MergeTree order by tuple(d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Variant(UInt64)) engine=MergeTree order by array(d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Variant(UInt64)) engine=MergeTree order by map('str', d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Variant(UInt64)) engine=MergeTree order by tuple() primary key d; -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Variant(UInt64)) engine=MergeTree order by tuple() partition by d; -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Variant(UInt64)) engine=MergeTree order by tuple() partition by tuple(d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Variant(UInt64)) engine=MergeTree order by tuple() partition by array(d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}
create table test (d Variant(UInt64)) engine=MergeTree order by tuple() partition by map('str', d); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}

create table test (d Dynamic) engine=Memory;
insert into test select * from numbers(5);

SET enable_analyzer=1;

set allow_suspicious_types_in_group_by=1;
set allow_suspicious_types_in_order_by=0;

select * from test order by d; -- {serverError ILLEGAL_COLUMN}
select * from test order by tuple(d); -- {serverError ILLEGAL_COLUMN}
select * from test order by array(d); -- {serverError ILLEGAL_COLUMN}
select * from test order by map('str', d); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_types_in_group_by=0;
set allow_suspicious_types_in_order_by=1;

select * from test group by d; -- {serverError ILLEGAL_COLUMN}
select * from test group by tuple(d); -- {serverError ILLEGAL_COLUMN}
select array(d) from test group by array(d); -- {serverError ILLEGAL_COLUMN}
select map('str', d) from test group by map('str', d); -- {serverError ILLEGAL_COLUMN}
select * from test group by grouping sets ((d), ('str')); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_types_in_group_by=1;
set allow_suspicious_types_in_order_by=1;

select * from test order by d;
select * from test order by tuple(d);
select * from test order by array(d);
select * from test order by map('str', d);

select * from test group by d order by all;
select * from test group by tuple(d) order by all;
select array(d) from test group by array(d) order by all;
select map('str', d) from test group by map('str', d) order by all;
select * from test group by grouping sets ((d), ('str')) order by all;

SET enable_analyzer=0;

set allow_suspicious_types_in_group_by=1;
set allow_suspicious_types_in_order_by=0;

select * from test order by d; -- {serverError ILLEGAL_COLUMN}
select * from test order by tuple(d); -- {serverError ILLEGAL_COLUMN}
select * from test order by array(d); -- {serverError ILLEGAL_COLUMN}
select * from test order by map('str', d); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_types_in_group_by=0;
set allow_suspicious_types_in_order_by=1;

select * from test group by d; -- {serverError ILLEGAL_COLUMN}
select * from test group by tuple(d); -- {serverError ILLEGAL_COLUMN}
select array(d) from test group by array(d); -- {serverError ILLEGAL_COLUMN}
select map('str', d) from test group by map('str', d); -- {serverError ILLEGAL_COLUMN}
select * from test group by grouping sets ((d), ('str')); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_types_in_group_by=1;
set allow_suspicious_types_in_order_by=1;

select * from test order by d;
select * from test order by tuple(d);
select * from test order by array(d);
select * from test order by map('str', d);

select * from test group by d order by all;
select * from test group by tuple(d) order by all;
select array(d) from test group by array(d) order by all;
select map('str', d) from test group by map('str', d) order by all;
select * from test group by grouping sets ((d), ('str')) order by all;

drop table test;

create table test (d Variant(UInt64)) engine=Memory;
insert into test select * from numbers(5);

SET enable_analyzer=1;

set allow_suspicious_types_in_group_by=1;
set allow_suspicious_types_in_order_by=0;

select * from test order by d; -- {serverError ILLEGAL_COLUMN}
select * from test order by tuple(d); -- {serverError ILLEGAL_COLUMN}
select * from test order by array(d); -- {serverError ILLEGAL_COLUMN}
select * from test order by map('str', d); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_types_in_group_by=0;
set allow_suspicious_types_in_order_by=1;

select * from test group by d; -- {serverError ILLEGAL_COLUMN}
select * from test group by tuple(d); -- {serverError ILLEGAL_COLUMN}
select array(d) from test group by array(d); -- {serverError ILLEGAL_COLUMN}
select map('str', d) from test group by map('str', d); -- {serverError ILLEGAL_COLUMN}
select * from test group by grouping sets ((d), ('str')); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_types_in_group_by=1;
set allow_suspicious_types_in_order_by=1;

select * from test order by d;
select * from test order by tuple(d);
select * from test order by array(d);
select * from test order by map('str', d);

select * from test group by d order by all;
select * from test group by tuple(d) order by all;
select array(d) from test group by array(d) order by all;
select map('str', d) from test group by map('str', d) order by all;
select * from test group by grouping sets ((d), ('str')) order by all;

SET enable_analyzer=0;

set allow_suspicious_types_in_group_by=1;
set allow_suspicious_types_in_order_by=0;

select * from test order by d; -- {serverError ILLEGAL_COLUMN}
select * from test order by tuple(d); -- {serverError ILLEGAL_COLUMN}
select * from test order by array(d); -- {serverError ILLEGAL_COLUMN}
select * from test order by map('str', d); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_types_in_group_by=0;
set allow_suspicious_types_in_order_by=1;

select * from test group by d; -- {serverError ILLEGAL_COLUMN}
select * from test group by tuple(d); -- {serverError ILLEGAL_COLUMN}
select array(d) from test group by array(d); -- {serverError ILLEGAL_COLUMN}
select map('str', d) from test group by map('str', d); -- {serverError ILLEGAL_COLUMN}
select * from test group by grouping sets ((d), ('str')); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_types_in_group_by=1;
set allow_suspicious_types_in_order_by=1;

select * from test order by d;
select * from test order by tuple(d);
select * from test order by array(d);
select * from test order by map('str', d);

select * from test group by d order by all;
select * from test group by tuple(d) order by all;
select array(d) from test group by array(d) order by all;
select map('str', d) from test group by map('str', d) order by all;
select * from test group by grouping sets ((d), ('str')) order by all;

drop table test;
