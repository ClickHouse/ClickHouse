-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- { echo }

SET enable_analyzer = 1;
SET optimize_or_like_chain = 0;

DROP TABLE IF EXISTS pruning_ref;
DROP TABLE IF EXISTS pruning_data;
DROP DICTIONARY IF EXISTS pruning_dict;

CREATE TABLE pruning_ref (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO pruning_ref VALUES (4242, 'match');

CREATE DICTIONARY pruning_dict (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'pruning_ref')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE pruning_data (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES pruning_data;
INSERT INTO pruning_data SELECT number FROM numbers(10000);

-- With optimization: predicate becomes `id IN (SELECT id FROM dictionary(...) WHERE name = 'match')`.
SET optimize_inverse_dictionary_lookup = 1;
EXPLAIN indexes = 1
SELECT count() FROM pruning_data
WHERE dictGetString('pruning_dict', 'name', id) = 'match';

-- Without optimization: `dictGet(...)` is stays, so all granules are scanned.
SET optimize_inverse_dictionary_lookup = 0;
EXPLAIN indexes = 1
SELECT count() FROM pruning_data
WHERE dictGetString('pruning_dict', 'name', id) = 'match';

-- Result are same
SET optimize_inverse_dictionary_lookup = 1;
SELECT count() FROM pruning_data
WHERE dictGetString('pruning_dict', 'name', id) = 'match';

SET optimize_inverse_dictionary_lookup = 0;
SELECT count() FROM pruning_data
WHERE dictGetString('pruning_dict', 'name', id) = 'match';

DROP DICTIONARY pruning_dict;
DROP TABLE pruning_data;
DROP TABLE pruning_ref;
