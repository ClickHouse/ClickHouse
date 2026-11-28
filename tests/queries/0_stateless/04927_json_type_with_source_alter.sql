-- Adding or removing `with_source` changes the set of streams in a data part,
-- so it must always run a mutation, even with lazy type hints enabled.

SET allow_experimental_json_lazy_type_hints = 1;

DROP TABLE IF EXISTS t_json_with_source_alter;
CREATE TABLE t_json_with_source_alter (id UInt64, json JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_with_source_alter VALUES (1, '{"a" : 42}');

-- Adding a type hint is metadata-only.
ALTER TABLE t_json_with_source_alter MODIFY COLUMN json JSON(a UInt32) SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_with_source_alter';

-- Adding the source is not.
ALTER TABLE t_json_with_source_alter MODIFY COLUMN json JSON(with_source=1, a UInt32) SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_with_source_alter';
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_json_with_source_alter' AND name = 'json';
SELECT id, json, json.a FROM t_json_with_source_alter ORDER BY id;

-- Removing it is not either.
ALTER TABLE t_json_with_source_alter MODIFY COLUMN json JSON(a UInt32) SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_with_source_alter';
SELECT id, json, json.a FROM t_json_with_source_alter ORDER BY id;

DROP TABLE t_json_with_source_alter;
