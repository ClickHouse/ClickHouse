-- Tags: no-parallel, log-engine

set enable_json_type=1;
set enable_dynamic_type=1;

drop table if exists test;
create table test (d Dynamic) engine=Log(); -- {serverError ILLEGAL_COLUMN}
create table test (d Dynamic) engine=TinyLog(); -- {serverError ILLEGAL_COLUMN}
create table test (d Array(Map(String, Dynamic))) engine=Log(); -- {serverError ILLEGAL_COLUMN}
create table test (d Array(Map(String, Dynamic))) engine=TinyLog(); -- {serverError ILLEGAL_COLUMN}
create table test (d JSON) engine=Log(); -- {serverError ILLEGAL_COLUMN}
create table test (d JSON) engine=TinyLog(); -- {serverError ILLEGAL_COLUMN}
create table test (d Array(Map(String, JSON))) engine=Log(); -- {serverError ILLEGAL_COLUMN}
create table test (d Array(Map(String, JSON))) engine=TinyLog(); -- {serverError ILLEGAL_COLUMN}
create table test (d Variant(Int32)) engine=Log(); -- {serverError ILLEGAL_COLUMN}
create table test (d Variant(Int32)) engine=TinyLog(); -- {serverError ILLEGAL_COLUMN}
create table test (d Array(Map(String, Variant(Int32)))) engine=Log(); -- {serverError ILLEGAL_COLUMN}
create table test (d Array(Map(String, Variant(Int32)))) engine=TinyLog(); -- {serverError ILLEGAL_COLUMN}

