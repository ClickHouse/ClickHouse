create view v as select number from numbers(5) where number%2={parity:Int8};
create table vv (number Int8) engine Merge(currentDatabase(),'v');
select * from vv; -- { serverError STORAGE_REQUIRES_PARAMETER }
