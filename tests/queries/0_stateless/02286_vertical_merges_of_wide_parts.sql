drop table if exists issue_36687_1;
drop table if exists issue_36687_2;
drop table if exists issue_36687_3;

CREATE TABLE issue_36687_1 ( a   Nullable(String), b  Int8)
ENGINE = MergeTree ORDER BY tuple() 
settings vertical_merge_algorithm_min_columns_to_activate=1,
    vertical_merge_algorithm_min_rows_to_activate=1,
    min_bytes_for_wide_part=0;

insert into issue_36687_1 select [], 0;
alter table issue_36687_1 clear column b;
optimize table issue_36687_1 final;
select * from issue_36687_1;


CREATE TABLE issue_36687_2 ( a   Nullable(String), b  Int8)
ENGINE = MergeTree ORDER BY tuple() 
settings vertical_merge_algorithm_min_columns_to_activate=1,
    vertical_merge_algorithm_min_rows_to_activate=1,
    min_bytes_for_wide_part=0;

insert into issue_36687_2 select Null, 0;
alter table issue_36687_2 clear column b;
optimize table issue_36687_2 final;
select * from issue_36687_2;



CREATE TABLE issue_36687_3 ( a   Nullable(String), b  Int8)
ENGINE = MergeTree ORDER BY tuple() 
settings vertical_merge_algorithm_min_columns_to_activate=1,
    vertical_merge_algorithm_min_rows_to_activate=1,
    min_bytes_for_wide_part=0;

insert into issue_36687_3 select Null, 0;
alter table issue_36687_3 add column c String;
optimize table issue_36687_3 final;
select * from issue_36687_3;


drop table issue_36687_1;
drop table issue_36687_2;
drop table issue_36687_3;
