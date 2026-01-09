-- Tags: no-replicated-database, no-parallel-replicas
-- no-parallel, no-parallel-replicas: Dictionary is not created in parallel replicas.

SET enable_analyzer = 1;
SET rewrite_in_to_join = 0;
SET prefer_localhost_replica = 1;

DROP DICTIONARY IF EXISTS inverse_dict_lookup_remote_shards;
CREATE DICTIONARY inverse_dict_lookup_remote_shards
(
  id Int64,
  f  Int64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY 'SELECT 1 id, 2 f'))
LAYOUT(flat())
LIFETIME(0);

SET optimize_inverse_dictionary_lookup = 0;
SELECT dictGet('inverse_dict_lookup_remote_shards', 'f', dummy) = 12 AS limit_and_equals
FROM remote('localhost,localhost', system.one)
LIMIT 1;

SET optimize_inverse_dictionary_lookup = 1;
SELECT dictGet('inverse_dict_lookup_remote_shards', 'f', dummy) = 12 AS limit_and_equals
FROM remote('localhost,localhost', system.one)
LIMIT 1;

DROP DICTIONARY IF EXISTS inverse_dict_lookup_remote_shards;
