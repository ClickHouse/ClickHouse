SET allow_experimental_data_skipping_indices = 1;

DROP TABLE IF EXISTS test.set_array;

CREATE TABLE test.set_array
(
    primary_key String,
    index_array Array(UInt64),
    INDEX additional_index_array (index_array) TYPE set(10000) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (primary_key);

INSERT INTO test.set_array
select
  toString(intDiv(number, 1000000)) as primary_key,
  array(number) as index_array
from system.numbers
limit 10000000;

SET max_rows_to_read = 8192;

select count() from test.set_array where has(index_array, 333);

DROP TABLE test.set_array;