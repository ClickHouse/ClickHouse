-- Tags: no-parallel-replicas
-- DirectKeyValueJoin (the algorithm under test) cannot be chosen with parallel replicas, so the
-- ParallelReplicas runner variant would throw NOT_IMPLEMENTED instead of exercising the fix.
-- Direct JOIN onto a dictionary with a wrapped (Nullable / LowCardinality / LowCardinality(Nullable))
-- left join key used to throw "Key type for complex key ... does not match" (TYPE_MISMATCH).
-- The direct-join lookup now normalizes the key to the dictionary's declared key schema, and a NULL
-- key never matches. See https://github.com/ClickHouse/ClickHouse/issues/111829

DROP DICTIONARY IF EXISTS dict_str;
DROP DICTIONARY IF EXISTS dict_empty;
DROP DICTIONARY IF EXISTS dict_nullable_key;
DROP TABLE IF EXISTS src_str;
DROP TABLE IF EXISTS src_empty;
DROP TABLE IF EXISTS src_nullable_key;
DROP DICTIONARY IF EXISTS dict_defaults;
DROP DICTIONARY IF EXISTS dict_date_key;
DROP TABLE IF EXISTS src_defaults;
DROP TABLE IF EXISTS src_date_key;

CREATE TABLE src_str (k String, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_str VALUES ('a', 'A'), ('b', 'B'), ('c', 'C');
CREATE DICTIONARY dict_str (k String, v String)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_str' DB currentDatabase())) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

-- Dictionary that has a real empty-string key: a NULL probe key must not coincide with it.
CREATE TABLE src_empty (k String, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_empty VALUES ('', 'EMPTYHIT'), ('a', 'A');
CREATE DICTIONARY dict_empty (k String, v String)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_empty' DB currentDatabase())) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

-- Dictionary whose declared key is itself Nullable: normalizing the probe wrapper must not break it.
CREATE TABLE src_nullable_key (k Nullable(UInt64), v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO src_nullable_key VALUES (1, 'one'), (2, 'two');
CREATE DICTIONARY dict_nullable_key (k Nullable(UInt64), v String)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_nullable_key' DB currentDatabase())) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

-- A Nullable key can also be carried by a sparse column or replicated by ARRAY JOIN; the lookup must
-- expose the null map through those special representations too.
CREATE TABLE src_sparse (k Nullable(String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO src_sparse VALUES ('a'), (NULL);

-- `k` is carried (not array-joined) through ARRAY JOIN of `arr`, so with lazy replication it becomes
-- a ColumnReplicated wrapping a Nullable column.
CREATE TABLE src_array (k Nullable(String), arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO src_array VALUES ('a', [1, 2]), (NULL, [3]);

SET join_algorithm = 'direct';

SELECT 'Nullable(String) key, LEFT, join_use_nulls=0';
SELECT t.pref, dd.v FROM (SELECT arrayJoin(['a', 'b', 'x', NULL]::Array(Nullable(String))) AS pref) AS t
LEFT JOIN dict_str AS dd ON t.pref = dd.k ORDER BY t.pref NULLS LAST, dd.v SETTINGS join_use_nulls = 0;

SELECT 'Nullable(String) key, LEFT, join_use_nulls=1';
SELECT t.pref, dd.v FROM (SELECT arrayJoin(['a', 'b', 'x', NULL]::Array(Nullable(String))) AS pref) AS t
LEFT JOIN dict_str AS dd ON t.pref = dd.k ORDER BY t.pref NULLS LAST, dd.v SETTINGS join_use_nulls = 1;

SELECT 'Nullable(String) key, INNER';
SELECT t.pref, dd.v FROM (SELECT arrayJoin(['a', 'b', 'x', NULL]::Array(Nullable(String))) AS pref) AS t
INNER JOIN dict_str AS dd ON t.pref = dd.k ORDER BY t.pref, dd.v;

SELECT 'NULL key does not match a real empty-string dictionary key';
SELECT t.pref, dd.v FROM (SELECT arrayJoin(['a', NULL]::Array(Nullable(String))) AS pref) AS t
LEFT JOIN dict_empty AS dd ON t.pref = dd.k ORDER BY t.pref NULLS LAST, dd.v;

SELECT 'A genuine empty-string key still matches';
SELECT t.pref, dd.v FROM (SELECT CAST('', 'Nullable(String)') AS pref) AS t
LEFT JOIN dict_empty AS dd ON t.pref = dd.k;

-- A constant NULL arrives as a ColumnConst, unlike the arrayJoin NULLs above; it must still not match.
SELECT 'A constant NULL key does not match the empty-string dictionary key';
SELECT t.pref, dd.v FROM (SELECT CAST(NULL, 'Nullable(String)') AS pref) AS t
LEFT JOIN dict_empty AS dd ON t.pref = dd.k;

-- Nullable-declared dictionary key with a Nullable probe: direct join is selected here, so the
-- wrapper normalization must keep both non-null lookups and the NULL non-match correct.
SELECT 'Dictionary with Nullable(UInt64) key, Nullable probe with NULL';
SELECT t.pref, dd.v FROM (SELECT arrayJoin([1, 2, NULL]::Array(Nullable(UInt64))) AS pref) AS t
LEFT JOIN dict_nullable_key AS dd ON t.pref = dd.k ORDER BY t.pref NULLS LAST, dd.v;

SELECT 'Nullable key carried by a sparse column, LEFT';
SELECT s.k, dd.v FROM src_sparse AS s
LEFT JOIN dict_str AS dd ON s.k = dd.k ORDER BY s.k NULLS LAST, dd.v;

SELECT 'Nullable key replicated by ARRAY JOIN, LEFT';
SELECT p.k, dd.v FROM (SELECT k FROM src_array ARRAY JOIN arr) AS p
LEFT JOIN dict_str AS dd ON p.k = dd.k ORDER BY p.k NULLS LAST, dd.v
SETTINGS enable_lazy_columns_replication = 1;

SELECT 'Direct join is still chosen for the Nullable key';
SELECT count() > 0 FROM (EXPLAIN actions = 1
    SELECT count() FROM (SELECT CAST('a', 'Nullable(String)') AS pref) AS t
    LEFT JOIN dict_str AS dd ON t.pref = dd.k)
WHERE explain ILIKE '%Algorithm: DirectKeyValueJoin%';

-- Not-matched rows get the type default, also for a type whose default is not the column zero.
CREATE TABLE src_defaults (k String, d Date32) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_defaults VALUES ('a', '2000-01-01');
CREATE DICTIONARY dict_defaults (k String, d Date32)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_defaults' DB currentDatabase())) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

CREATE TABLE src_date_key (k Date32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_date_key VALUES ('2000-01-01', 'A');
CREATE DICTIONARY dict_date_key (k Date32, v String)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_date_key' DB currentDatabase())) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

SELECT 'Type defaults of not-matched rows, Nullable key';
SELECT t.pref, dd.d FROM (SELECT arrayJoin(['a', 'b', NULL]::Array(Nullable(String))) AS pref) AS t
LEFT JOIN dict_defaults AS dd ON t.pref = dd.k ORDER BY t.pref NULLS LAST SETTINGS join_use_nulls = 0;

SELECT 'Type defaults of not-matched rows, non-Nullable key';
SELECT t.pref, dd.d FROM (SELECT arrayJoin(['a', 'b']) AS pref) AS t
LEFT JOIN dict_defaults AS dd ON t.pref = dd.k ORDER BY t.pref SETTINGS join_use_nulls = 0;

SELECT 'Type default of a not-found Date32 dictionary key';
SELECT t.k, dd.k, dd.v FROM (SELECT arrayJoin([toDate32('2000-01-01'), toDate32('2000-01-02')]) AS k) AS t
LEFT JOIN dict_date_key AS dd ON t.k = dd.k ORDER BY t.k SETTINGS join_use_nulls = 0;

DROP DICTIONARY dict_str;
DROP DICTIONARY dict_empty;
DROP DICTIONARY dict_nullable_key;
DROP TABLE src_str;
DROP TABLE src_empty;
DROP TABLE src_nullable_key;
DROP TABLE src_sparse;
DROP TABLE src_array;
DROP DICTIONARY dict_defaults;
DROP DICTIONARY dict_date_key;
DROP TABLE src_defaults;
DROP TABLE src_date_key;
