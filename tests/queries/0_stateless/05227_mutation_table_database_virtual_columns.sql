-- Mutations on MergeTree read parts through MergeTreeSequentialSource, which must provide
-- the constant virtual columns `_table` and `_database` like a regular SELECT does.
-- https://github.com/ClickHouse/ClickHouse/issues/102331

DROP TABLE IF EXISTS t_mutation_table_virtual;

CREATE TABLE t_mutation_table_virtual (key Int, value String, db String DEFAULT '')
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_mutation_table_virtual (key, value) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

ALTER TABLE t_mutation_table_virtual UPDATE value = _table WHERE key = 1 SETTINGS mutations_sync = 1;
ALTER TABLE t_mutation_table_virtual UPDATE db = _database WHERE key = 2 SETTINGS mutations_sync = 1;
ALTER TABLE t_mutation_table_virtual DELETE WHERE _table = 't_mutation_table_virtual' AND key = 3 SETTINGS mutations_sync = 1;

SELECT key, value, db = currentDatabase() FROM t_mutation_table_virtual ORDER BY key;

-- Lightweight delete also goes through a mutation.
DELETE FROM t_mutation_table_virtual WHERE _database != '' AND key = 4;
SELECT key, value FROM t_mutation_table_virtual ORDER BY key;

DROP TABLE t_mutation_table_virtual;
