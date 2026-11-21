-- A new stored definition qualifies the dictionary name of `assignCentroid` with its own database, like the one of `dictGet`,
-- so the view, the column expression or the mutation does not depend on the current database of the query that uses it.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE centroids (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO centroids VALUES (0, [0.0, 0.0]), (7, [10.0, 10.0]);
CREATE DICTIONARY centroids_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'centroids')) LAYOUT(FLAT(MAX_ARRAY_SIZE 1000)) LIFETIME(0);

CREATE TABLE vectors (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO vectors VALUES (1, [1.0, 1.0]), (2, [9.0, 9.0]);

CREATE VIEW v_bound AS SELECT id, assignCentroid(v, 'centroids_dict') AS c FROM vectors;
CREATE TABLE mv_src (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_dst (id UInt32, c UInt32) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv TO mv_dst AS SELECT id, assignCentroid(v, 'centroids_dict') AS c FROM mv_src;
CREATE TABLE t_default
(
    id UInt32,
    v Array(Float32),
    c UInt32 DEFAULT assignCentroid(v, 'centroids_dict'),
    m UInt32 MATERIALIZED assignCentroid(v, 'centroids_dict')
)
ENGINE = MergeTree ORDER BY id;
ALTER TABLE vectors ADD COLUMN a UInt32 ALIAS assignCentroid(v, 'centroids_dict');

-- A mutation is executed in the server context, whose current database is not this one.
CREATE TABLE t_mut (id UInt32, v Array(Float32), c UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mut VALUES (1, [1.0, 1.0], 99), (2, [9.0, 9.0], 99);
ALTER TABLE t_mut UPDATE c = assignCentroid(v, 'centroids_dict') WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, c FROM t_mut ORDER BY id;

SELECT name, countSubstrings(create_table_query, concat('''', currentDatabase(), '.centroids_dict''')) FROM system.tables
WHERE database = currentDatabase() AND name IN ('v_bound', 'mv', 't_default', 'vectors') ORDER BY name;

USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.v_bound ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.mv_src VALUES (1, [1.0, 1.0]), (2, [9.0, 9.0]);
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.mv_dst ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.t_default (id, v) VALUES (1, [1.0, 1.0]), (2, [9.0, 9.0]);
SELECT id, c, m FROM {CLICKHOUSE_DATABASE:Identifier}.t_default ORDER BY id;
SELECT id, a FROM {CLICKHOUSE_DATABASE:Identifier}.vectors ORDER BY id;

-- An ALTER issued from another current database binds the name to the database of the table.
ALTER TABLE {CLICKHOUSE_DATABASE:Identifier}.vectors ADD COLUMN a_other UInt32 ALIAS assignCentroid(v, 'centroids_dict');
SELECT id, a_other FROM {CLICKHOUSE_DATABASE:Identifier}.vectors ORDER BY id;
ALTER TABLE {CLICKHOUSE_DATABASE:Identifier}.t_mut UPDATE c = assignCentroid(v, 'centroids_dict') + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, c FROM {CLICKHOUSE_DATABASE:Identifier}.t_mut ORDER BY id;

-- A dictionary with the same name in the current database is not used instead.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.other_centroids (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.other_centroids VALUES (3, [1.0, 1.0]), (5, [9.0, 9.0]);
CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.centroids_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'other_centroids')) LAYOUT(FLAT(MAX_ARRAY_SIZE 1000)) LIFETIME(0);
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.v_bound ORDER BY id;
ALTER TABLE {CLICKHOUSE_DATABASE:Identifier}.vectors MODIFY COLUMN a_other UInt32 ALIAS assignCentroid(v, 'centroids_dict');
SELECT id, a_other FROM {CLICKHOUSE_DATABASE:Identifier}.vectors ORDER BY id;

-- A definition that is not new keeps the name as it is stored: a name that did not resolve when the view was created is
-- still resolved in the current database of the reader after a short ATTACH and after a RESTORE.
USE {CLICKHOUSE_DATABASE:Identifier};
CREATE VIEW v_unbound AS SELECT id, assignCentroid(v, 'late_dict') AS c FROM vectors;
CREATE DICTIONARY late_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'centroids')) LAYOUT(FLAT(MAX_ARRAY_SIZE 1000)) LIFETIME(0);
CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.late_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'other_centroids')) LAYOUT(FLAT(MAX_ARRAY_SIZE 1000)) LIFETIME(0);
DETACH VIEW v_unbound PERMANENTLY;
ATTACH TABLE v_unbound;
BACKUP TABLE v_unbound TO Memory('05353_unbound') FORMAT Null;
RESTORE TABLE v_unbound AS v_unbound_restored FROM Memory('05353_unbound') FORMAT Null;
SELECT countSubstrings(create_table_query, '''late_dict''') FROM system.tables WHERE database = currentDatabase() AND name = 'v_unbound_restored';
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.v_unbound ORDER BY id;
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.v_unbound_restored ORDER BY id;

-- Restoring the dictionary under a new name renames the qualified name.
USE {CLICKHOUSE_DATABASE:Identifier};
BACKUP TABLE v_bound, DICTIONARY centroids_dict TO Memory('05353_bound') FORMAT Null;
RESTORE TABLE v_bound AS v_restored, DICTIONARY centroids_dict AS centroids_dict_restored FROM Memory('05353_bound') FORMAT Null;
SELECT countSubstrings(create_table_query, concat('''', currentDatabase(), '.centroids_dict_restored''')) FROM system.tables
WHERE database = currentDatabase() AND name = 'v_restored';
DROP DICTIONARY centroids_dict;
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.v_restored ORDER BY id;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
