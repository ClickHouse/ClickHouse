DROP TABLE IF EXISTS test_string;
DROP TABLE IF EXISTS test_string_alias;
CREATE OR REPLACE TABLE test_string
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id settings add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=1;

CREATE OR REPLACE TABLE test_string_alias
(
    id UInt64,
    unhexed String ALIAS 'abc',
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id settings add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=1;

SELECT table, name FROM system.data_skipping_indices WHERE database = currentDatabase() ORDER BY table, name;

DROP TABLE test_string;
DROP TABLE test_string_alias;