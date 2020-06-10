SET check_query_single_value_result = 'false';

DROP TABLE IF EXISTS check_table_with_indices;

CREATE TABLE check_table_with_indices (
  id UInt64,
  data String,
  INDEX a (id) type minmax GRANULARITY 3
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO check_table_with_indices VALUES (0, 'test'), (1, 'test2');

CHECK TABLE check_table_with_indices;

DROP TABLE check_table_with_indices;
