-- Tags: distributed

-- Force using skip indexes in planning to make test deterministic with max_rows_to_read.
set use_skip_indexes_on_data_read = 0;

create table table_1 (x UInt32, y String, INDEX a (length(y)) TYPE minmax GRANULARITY 1) engine = MergeTree order by x settings index_granularity = 2;
insert into table_1 values (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');

CREATE TABLE distr_table (x UInt32, y String) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), 'table_1');

-- { echoOn }
set max_rows_to_read = 2;

select * from table_1 order by x settings additional_table_filters={'table_1' : 'x > 3'};
select * from table_1 order by x settings additional_table_filters={'table_1' : 'x < 3'};

select * from table_1 order by x settings additional_table_filters={'table_1' : 'length(y) >= 3'};
select * from table_1 order by x settings additional_table_filters={'table_1' : 'length(y) < 3'};

set max_rows_to_read = 4;

select * from distr_table order by x settings additional_table_filters={'distr_table' : 'x > 3'};
select * from distr_table order by x settings additional_table_filters={'distr_table' : 'x < 3'};

select * from distr_table order by x settings additional_table_filters={'distr_table' : 'length(y) > 3'};
select * from distr_table order by x settings additional_table_filters={'distr_table' : 'length(y) < 3'};

set param_a = 3;

set max_rows_to_read = 2;

select * from table_1 order by x settings additional_table_filters={'table_1' : 'x > {a:UInt32}'};
select * from table_1 order by x settings additional_table_filters={'table_1' : 'x < {a:UInt32}'};

select * from table_1 order by x settings additional_table_filters={'table_1' : 'length(y) >= {a:UInt32}'};
select * from table_1 order by x settings additional_table_filters={'table_1' : 'length(y) < {a:UInt32}'};

set max_rows_to_read = 4;

select * from distr_table order by x settings additional_table_filters={'distr_table' : 'x > {a:UInt32}'};
select * from distr_table order by x settings additional_table_filters={'distr_table' : 'x < {a:UInt32}'};

select * from distr_table order by x settings additional_table_filters={'distr_table' : 'length(y) > {a:UInt32}'};
select * from distr_table order by x settings additional_table_filters={'distr_table' : 'length(y) < {a:UInt32}'};
