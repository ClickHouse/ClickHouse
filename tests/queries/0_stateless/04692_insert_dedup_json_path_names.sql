-- Insert deduplication hashes a JSON object by walking its paths in sorted order and hashing each
-- path name next to its values. Hashing the values alone left the hash blind to which path they
-- belong to, so `{"a":1}` and `{"b":1}` collided and the second insert was silently dropped.

SET enable_json_type = 1;
SET max_insert_threads = 1;

DROP TABLE IF EXISTS t_dedup_json_same;
DROP TABLE IF EXISTS t_dedup_json_renamed;
DROP TABLE IF EXISTS t_dedup_json_renamed_pair;
DROP TABLE IF EXISTS t_dedup_json_renamed_string;
DROP TABLE IF EXISTS t_dedup_json_renamed_nested;
DROP TABLE IF EXISTS t_dedup_json_other_value;
DROP TABLE IF EXISTS t_dedup_json_key_order;
DROP TABLE IF EXISTS t_dedup_json_sync;
DROP TABLE IF EXISTS t_dedup_json_select;
DROP TABLE IF EXISTS t_dedup_json_no_dedup;

-- The same object twice must still deduplicate.
CREATE TABLE t_dedup_json_same (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_same VALUES (1, '{"a":1}');
INSERT INTO t_dedup_json_same VALUES (1, '{"a":1}');
SELECT 'identical object deduplicated', count() FROM t_dedup_json_same;

-- Same value under a different path name: two different objects, both must be kept.
CREATE TABLE t_dedup_json_renamed (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_renamed VALUES (1, '{"a":1}');
INSERT INTO t_dedup_json_renamed VALUES (1, '{"b":1}');
SELECT 'renamed path kept', count() FROM t_dedup_json_renamed;
SELECT 'renamed path contents', data FROM t_dedup_json_renamed ORDER BY toString(data);

-- Every path renamed, values unchanged.
CREATE TABLE t_dedup_json_renamed_pair (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_renamed_pair VALUES (1, '{"a":1,"b":2}');
INSERT INTO t_dedup_json_renamed_pair VALUES (1, '{"c":1,"d":2}');
SELECT 'both paths renamed kept', count() FROM t_dedup_json_renamed_pair;

-- The same with a String value, which hashes through a different nested column.
CREATE TABLE t_dedup_json_renamed_string (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_renamed_string VALUES (1, '{"x":"s"}');
INSERT INTO t_dedup_json_renamed_string VALUES (1, '{"y":"s"}');
SELECT 'renamed String path kept', count() FROM t_dedup_json_renamed_string;

-- A renamed path one level down, so the differing path names are nested ones.
CREATE TABLE t_dedup_json_renamed_nested (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_renamed_nested VALUES (1, '{"a":{"b":1}}');
INSERT INTO t_dedup_json_renamed_nested VALUES (1, '{"c":{"d":1}}');
SELECT 'renamed nested path kept', count() FROM t_dedup_json_renamed_nested;

-- Control: a differing value was always caught, because the value is hashed.
CREATE TABLE t_dedup_json_other_value (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_other_value VALUES (1, '{"a":1}');
INSERT INTO t_dedup_json_other_value VALUES (1, '{"b":2}');
SELECT 'renamed path with other value kept', count() FROM t_dedup_json_other_value;

-- Paths are hashed in sorted order, so the key order of the input text must not matter.
CREATE TABLE t_dedup_json_key_order (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_key_order VALUES (1, '{"a":1,"b":2}');
INSERT INTO t_dedup_json_key_order VALUES (1, '{"b":2,"a":1}');
SELECT 'reordered keys deduplicated', count() FROM t_dedup_json_key_order;

-- The hash is shared by the async and the synchronous insert path, so it must hold with
-- async_insert disabled too.
CREATE TABLE t_dedup_json_sync (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_sync SETTINGS async_insert = 0 VALUES (1, '{"a":1}');
INSERT INTO t_dedup_json_sync SETTINGS async_insert = 0 VALUES (1, '{"b":1}');
SELECT 'renamed path kept without async_insert', count() FROM t_dedup_json_sync;

-- INSERT SELECT reaches the same hash once deduplication is not declined for an unordered query.
CREATE TABLE t_dedup_json_select (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_json_select SETTINGS deduplicate_insert_select = 'enable_even_for_bad_queries'
SELECT 1, materialize('{"a":1}')::JSON;
INSERT INTO t_dedup_json_select SETTINGS deduplicate_insert_select = 'enable_even_for_bad_queries'
SELECT 1, materialize('{"b":1}')::JSON;
SELECT 'renamed path kept for INSERT SELECT', count() FROM t_dedup_json_select;

-- Without a deduplication window the same object must append.
CREATE TABLE t_dedup_json_no_dedup (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dedup_json_no_dedup VALUES (1, '{"a":1}');
INSERT INTO t_dedup_json_no_dedup VALUES (1, '{"a":1}');
SELECT 'insert without deduplication appends', count() FROM t_dedup_json_no_dedup;

DROP TABLE t_dedup_json_no_dedup;
DROP TABLE t_dedup_json_select;
DROP TABLE t_dedup_json_sync;
DROP TABLE t_dedup_json_key_order;
DROP TABLE t_dedup_json_other_value;
DROP TABLE t_dedup_json_renamed_nested;
DROP TABLE t_dedup_json_renamed_string;
DROP TABLE t_dedup_json_renamed_pair;
DROP TABLE t_dedup_json_renamed;
DROP TABLE t_dedup_json_same;
