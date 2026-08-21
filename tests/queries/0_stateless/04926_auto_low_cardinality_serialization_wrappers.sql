SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS source_default;
CREATE TABLE source_default
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0;

INSERT INTO source_default SELECT number, if(number % 2 = 0, '', 'value') FROM numbers(2000);

DROP TABLE IF EXISTS source_low_cardinality;
CREATE TABLE source_low_cardinality
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

INSERT INTO source_low_cardinality SELECT number, if(number % 2 = 0, '', 'value') FROM numbers(2000);

DROP TABLE IF EXISTS all_tables;
CREATE TABLE all_tables AS source_low_cardinality
ENGINE = Merge(currentDatabase(), 'source_.*');

DROP TABLE IF EXISTS alias_all_tables;
CREATE TABLE alias_all_tables
ENGINE = Alias('all_tables');

SELECT count() FROM all_tables WHERE empty(lc);
SELECT count() FROM alias_all_tables WHERE empty(lc);

DROP TABLE alias_all_tables;
DROP TABLE all_tables;
DROP TABLE source_low_cardinality;
DROP TABLE source_default;
