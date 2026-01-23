-- Tags: no-parallel-replicas
-- no-parallel-replicas: Different plan
-- Implicit indices should not prevent any ALTERs, even if `alter_column_secondary_index_mode` is set to 'throw'

SET enable_analyzer=1; -- Different plan

DROP TABLE IF EXISTS test_alter;
CREATE TABLE test_alter (
      a Int32,
      b Int32,
)
ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=1, alter_column_secondary_index_mode='throw';

INSERT INTO test_alter VALUES (1, 1);
INSERT INTO test_alter VALUES (2, 2);

SELECT name, type_full, expr, creation, data_compressed_bytes > 0 FROM system.data_skipping_indices where database = current_database() and table = 'test_alter' ORDER BY name;
ALTER TABLE test_alter MODIFY COLUMN b String;
SELECT name, type_full, expr, creation, data_compressed_bytes > 0 FROM system.data_skipping_indices where database = current_database() and table = 'test_alter' ORDER BY name;
EXPLAIN indexes=1 SELECT * FROM test_alter WHERE b = '2';