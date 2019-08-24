SET allow_experimental_data_skipping_indices = 1;
DROP TABLE IF EXISTS  test.skip_set;
CREATE TABLE test.skip_set
( x UInt64,
  INDEX x_idx x TYPE set (0) GRANULARITY 2
)
ENGINE = MergeTree () order BY tuple()
SETTINGS index_granularity = 8192;

insert into test.skip_set (x) select number from numbers(100000);

SET max_rows_to_read = 16384;

select count() from test.skip_set where x in (1,2,3);
select count() from test.skip_set where x in (select toUInt64(1));