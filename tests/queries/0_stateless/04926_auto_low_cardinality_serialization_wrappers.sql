SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP DATABASE IF EXISTS auto_lc_wrappers;
CREATE DATABASE auto_lc_wrappers;

CREATE TABLE auto_lc_wrappers.source
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0;

INSERT INTO auto_lc_wrappers.source SELECT number, if(number % 2 = 0, '', 'value') FROM numbers(2000);

CREATE TABLE auto_lc_wrappers.all_tables AS auto_lc_wrappers.source
ENGINE = Merge('auto_lc_wrappers', 'source');

SELECT count() FROM auto_lc_wrappers.all_tables WHERE empty(lc);

DROP DATABASE auto_lc_wrappers;
