drop view if exists view_columns_check_v1;
drop view if exists view_columns_check_v2;
drop view if exists view_columns_check_v3;

create view view_columns_check_v1 (a UInt64) as select number as a from system.numbers limit 1;
create view view_columns_check_v2 (a String) as select number as a from system.numbers limit 1; -- { serverError 80 }
create view view_columns_check_v3 (b UInt64) as select number as a from system.numbers limit 1; -- { serverError 80 }

drop view if exists view_columns_check_v1;
drop view if exists view_columns_check_v2;
drop view if exists view_columns_check_v3;
