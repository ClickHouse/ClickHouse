-- Tags: no-replicated-database, no-parallel-replicas
-- no-parallel, no-parallel-replicas: Dictionary is not created in parallel replicas.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_or_like_chain = 0;

DROP DICTIONARY IF EXISTS dictionary_all;
DROP TABLE IF EXISTS ref_table_all;
DROP TABLE IF EXISTS tab;

CREATE TABLE ref_table_all
(
  id   UInt64,
  name String,
  i8   String,
  i16  String,
  i32  String,
  i64  String,
  u8   String,
  u16  String,
  u32  String,
  u64  String,
  f32  String,
  f64  String,
  d    String,
  dt   String,
  uid  String,
  ip4  String,
  ip6  String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO ref_table_all VALUES
    (1, 'alpha', -8, -16, -32, -64, 8, 16, 32, 64, 10.0, 20.0,
     '2025-01-01', '2025-01-01 10:00:00',
     '00000000-0000-0000-0000-000000000001',
     '192.168.0.1', '2001:db8::1'),
    (2, 'beta', -7, -15, -31, -63, 9, 17, 33, 65, 11.0, 21.0,
     '2026-01-01', '2026-01-01 15:00:00',
     '00000000-0000-0000-0000-000000000002',
     '10.0.0.3', '2001:db8::2');

CREATE DICTIONARY dictionary_all
(
  id   UInt64,
  name String,
  i8   String,
  i16  String,
  i32  String,
  i64  String,
  u8   String,
  u16  String,
  u32  String,
  u64  String,
  f32  String,
  f64  String,
  d    String,
  dt   String,
  uid  String,
  ip4  String,
  ip6  String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_table_all'))
LAYOUT(HASHED())
LIFETIME(0);

CREATE TABLE tab
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1,'x'),(2,'y'),(99,'z');

SELECT 'dictGet (generic) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGet('dictionary_all', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'dictGet (generic)';
SELECT id, payload FROM tab
WHERE dictGet('dictionary_all', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'dictGetString - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetString('dictionary_all', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'dictGetString';
SELECT id, payload FROM tab
WHERE dictGetString('dictionary_all', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'dictGetInt32 - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetInt32('dictionary_all', 'i32', id) = -32
ORDER BY id, payload;

SELECT 'dictGetInt32';
SELECT id, payload FROM tab
WHERE dictGetInt32('dictionary_all', 'i32', id) = -32
ORDER BY id, payload;

SELECT 'dictGetUInt64 - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetUInt64('dictionary_all', 'u64', id) = 64
ORDER BY id, payload;

SELECT 'dictGetUInt64';
SELECT id, payload FROM tab
WHERE dictGetUInt64('dictionary_all', 'u64', id) = 64
ORDER BY id, payload;

SELECT 'dictGetFloat64 - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetFloat64('dictionary_all', 'f64', id) = 20.0
ORDER BY id, payload;

SELECT 'dictGetFloat64';
SELECT id, payload FROM tab
WHERE dictGetFloat64('dictionary_all', 'f64', id) = 20.0
ORDER BY id, payload;

SELECT 'dictGetDate - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetDate('dictionary_all', 'd', id) = toDate('2025-01-01')
ORDER BY id, payload;

SELECT 'dictGetDate';
SELECT id, payload FROM tab
WHERE dictGetDate('dictionary_all', 'd', id) = toDate('2025-01-01')
ORDER BY id, payload;

SELECT 'dictGetDateTime - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetDateTime('dictionary_all', 'dt', id) = toDateTime('2025-01-01 10:00:00')
ORDER BY id, payload;

SELECT 'dictGetDateTime';
SELECT id, payload FROM tab
WHERE dictGetDateTime('dictionary_all', 'dt', id) = toDateTime('2025-01-01 10:00:00')
ORDER BY id, payload;

SELECT 'dictGetUUID - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetUUID('dictionary_all', 'uid', id) = toUUID('00000000-0000-0000-0000-000000000001')
ORDER BY id, payload;

SELECT 'dictGetUUID';
SELECT id, payload FROM tab
WHERE dictGetUUID('dictionary_all', 'uid', id) = toUUID('00000000-0000-0000-0000-000000000001')
ORDER BY id, payload;

SELECT 'dictGetIPv4 - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetIPv4('dictionary_all', 'ip4', id) = toIPv4('192.168.0.1')
ORDER BY id, payload;

SELECT 'dictGetIPv4';
SELECT id, payload FROM tab
WHERE dictGetIPv4('dictionary_all', 'ip4', id) = toIPv4('192.168.0.1')
ORDER BY id, payload;

SELECT 'dictGetIPv6 - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetIPv6('dictionary_all', 'ip6', id) = toIPv6('2001:db8::1')
ORDER BY id, payload;

SELECT 'dictGetIPv6';
SELECT id, payload FROM tab
WHERE dictGetIPv6('dictionary_all', 'ip6', id) = toIPv6('2001:db8::1')
ORDER BY id, payload;

SELECT 'dictGetOrNull(String) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE dictGetOrNull('dictionary_all', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'dictGetOrNull(String)';
SELECT id, payload FROM tab
WHERE dictGetOrNull('dictionary_all', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'dictGetOrNull(String) IS NULL - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload FROM tab
WHERE isNull(dictGetOrNull('dictionary_all','name', id))
ORDER BY id, payload;

SELECT 'dictGetOrNull(String) IS NULL';
SELECT id, payload FROM tab
WHERE isNull(dictGetOrNull('dictionary_all','name', id))
ORDER BY id, payload;
