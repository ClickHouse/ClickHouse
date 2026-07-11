-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS set_array;

CREATE TABLE set_array
(
    primary_key String,
    index_array Array(UInt64),
    INDEX additional_index_array (index_array) TYPE set(10000) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (primary_key);

INSERT INTO set_array
select
  toString(intDiv(number, 100000)) as primary_key,
  array(number) as index_array
from system.numbers
limit 1000000;

OPTIMIZE TABLE set_array FINAL;

SET max_rows_to_read = 8192;

select count() from set_array where has(index_array, 333);

DROP TABLE set_array;
