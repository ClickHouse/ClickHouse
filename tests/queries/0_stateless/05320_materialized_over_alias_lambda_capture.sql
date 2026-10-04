-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation the on-fly case needs to stay pending

-- An ALIAS definition expanded into a MATERIALIZED default is written at table scope, so a lambda parameter
-- with the same name must not capture it, and `p.f` inside a lambda is the field `f` of the parameter `p`.
-- Each line prints the stored columns, then the same expressions computed by SELECT.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_alias_capture;
CREATE TABLE t_alias_capture
(
    k UInt32,
    arr Array(UInt32),
    d UInt32 ALIAS k + 1,
    e UInt32 ALIAS d * 10,
    d2 Array(UInt32) ALIAS arrayMap(k -> d, arr),
    m_renamed Array(UInt32) MATERIALIZED arrayMap(x -> d, arr),
    m_shadow Array(UInt32) MATERIALIZED arrayMap(k -> d, arr),
    m_inner Array(UInt32) MATERIALIZED d2,
    m_param Array(UInt32) MATERIALIZED arrayMap(d -> d + e, arr),
    m_chain Array(UInt32) MATERIALIZED arrayMap(k -> e, arr)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture (k, arr) VALUES (5, [100]);
SELECT 'insert', m_renamed, m_shadow, m_inner, m_param, m_chain,
    arrayMap(x -> d, arr), arrayMap(k -> d, arr), d2, arrayMap(d -> d + e, arr), arrayMap(k -> e, arr) FROM t_alias_capture;

ALTER TABLE t_alias_capture UPDATE arr = [200] WHERE 1;
SELECT 'update arr', m_renamed, m_shadow, m_inner, m_param, m_chain,
    arrayMap(x -> d, arr), arrayMap(k -> d, arr), d2, arrayMap(d -> d + e, arr), arrayMap(k -> e, arr) FROM t_alias_capture;

-- `k` is read only through the expanded definitions, so updating it has to recompute every column.
ALTER TABLE t_alias_capture UPDATE k = 7 WHERE 1;
SELECT 'update k', m_renamed, m_shadow, m_inner, m_param, m_chain,
    arrayMap(x -> d, arr), arrayMap(k -> d, arr), d2, arrayMap(d -> d + e, arr), arrayMap(k -> e, arr) FROM t_alias_capture;

DROP TABLE t_alias_capture;

-- On-fly read of a pending mutation.
DROP TABLE IF EXISTS t_alias_capture_fly SYNC;
CREATE TABLE t_alias_capture_fly
(
    k UInt32,
    arr Array(UInt32),
    d UInt32 ALIAS k + 1,
    m_renamed Array(UInt32) MATERIALIZED arrayMap(x -> d, arr),
    m_shadow Array(UInt32) MATERIALIZED arrayMap(k -> d, arr)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_fly (k, arr) VALUES (5, [100]);
SYSTEM STOP MERGES t_alias_capture_fly;
ALTER TABLE t_alias_capture_fly UPDATE k = 7 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'on fly', m_renamed, m_shadow, arrayMap(k -> d, arr) FROM t_alias_capture_fly SETTINGS apply_mutations_on_fly = 1;
SYSTEM START MERGES t_alias_capture_fly;
DROP TABLE t_alias_capture_fly SYNC;

-- CLEAR COLUMN resets `k` to its default.
DROP TABLE IF EXISTS t_alias_capture_clear;
CREATE TABLE t_alias_capture_clear
(
    k UInt32,
    arr Array(UInt32),
    d UInt32 ALIAS k + 1,
    m_renamed Array(UInt32) MATERIALIZED arrayMap(x -> d, arr),
    m_shadow Array(UInt32) MATERIALIZED arrayMap(k -> d, arr)
)
ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple();

INSERT INTO t_alias_capture_clear (k, arr) VALUES (5, [100]);
ALTER TABLE t_alias_capture_clear CLEAR COLUMN k IN PARTITION tuple();
SELECT 'clear column', k, m_renamed, m_shadow, arrayMap(k -> d, arr) FROM t_alias_capture_clear;
DROP TABLE t_alias_capture_clear;

-- A column TTL resets `k`, in a merge and in MATERIALIZE TTL.
DROP TABLE IF EXISTS t_alias_capture_ttl;
CREATE TABLE t_alias_capture_ttl
(
    t DateTime,
    k UInt32 TTL t + INTERVAL 1 SECOND,
    arr Array(UInt32),
    d UInt32 ALIAS k + 1,
    m_renamed Array(UInt32) MATERIALIZED arrayMap(x -> d, arr),
    m_shadow Array(UInt32) MATERIALIZED arrayMap(k -> d, arr)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_alias_capture_ttl (t, k, arr) VALUES ('2000-01-01 00:00:00', 5, [100]);
OPTIMIZE TABLE t_alias_capture_ttl FINAL;
SELECT 'merge ttl', k, m_renamed, m_shadow, arrayMap(k -> d, arr) FROM t_alias_capture_ttl;
DROP TABLE t_alias_capture_ttl;

DROP TABLE IF EXISTS t_alias_capture_materialize_ttl;
CREATE TABLE t_alias_capture_materialize_ttl
(
    t DateTime,
    k UInt32,
    arr Array(UInt32),
    d UInt32 ALIAS k + 1,
    m_renamed Array(UInt32) MATERIALIZED arrayMap(x -> d, arr),
    m_shadow Array(UInt32) MATERIALIZED arrayMap(k -> d, arr)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_alias_capture_materialize_ttl (t, k, arr) VALUES ('2000-01-01 00:00:00', 5, [100]);
ALTER TABLE t_alias_capture_materialize_ttl MODIFY COLUMN k UInt32 TTL t + INTERVAL 1 SECOND SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_alias_capture_materialize_ttl MATERIALIZE TTL;
SELECT 'materialize ttl', k, m_renamed, m_shadow, arrayMap(k -> d, arr) FROM t_alias_capture_materialize_ttl;
DROP TABLE t_alias_capture_materialize_ttl;

-- Lightweight UPDATE writes a patch part, and the merge applies it.
DROP TABLE IF EXISTS t_alias_capture_patch;
CREATE TABLE t_alias_capture_patch
(
    k UInt32,
    arr Array(UInt32),
    d UInt32 ALIAS k + 1,
    m_renamed Array(UInt32) MATERIALIZED arrayMap(x -> d, arr),
    m_shadow Array(UInt32) MATERIALIZED arrayMap(k -> d, arr)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_alias_capture_patch (k, arr) VALUES (5, [100]);
SET enable_lightweight_update = 1;
UPDATE t_alias_capture_patch SET k = 7 WHERE 1;
SELECT 'lightweight update', k, m_renamed, m_shadow, arrayMap(k -> d, arr) FROM t_alias_capture_patch;
OPTIMIZE TABLE t_alias_capture_patch FINAL;
SELECT 'patch applied', k, m_renamed, m_shadow, arrayMap(k -> d, arr) FROM t_alias_capture_patch;
DROP TABLE t_alias_capture_patch;

-- `k.a` is the field of the parameter, and the ALIAS reads the Tuple column `k`.
DROP TABLE IF EXISTS t_alias_capture_field;
CREATE TABLE t_alias_capture_field
(
    k Tuple(a UInt32),
    z UInt32,
    arr_t Array(Tuple(a UInt32)),
    d UInt32 ALIAS k.a + 1,
    m Array(UInt32) MATERIALIZED arrayMap(k -> k.a + d, arr_t)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_field (k, z, arr_t) VALUES (tuple(5), 0, [tuple(100)]);
ALTER TABLE t_alias_capture_field UPDATE z = 1 WHERE 1;
SELECT 'field update z', z, m, arrayMap(k -> k.a + d, arr_t) FROM t_alias_capture_field;
ALTER TABLE t_alias_capture_field UPDATE arr_t = [tuple(200)] WHERE 1;
SELECT 'field update arr_t', m, arrayMap(k -> k.a + d, arr_t) FROM t_alias_capture_field;
ALTER TABLE t_alias_capture_field UPDATE k = tuple(7) WHERE 1;
SELECT 'field update k', m, arrayMap(k -> k.a + d, arr_t) FROM t_alias_capture_field;
DROP TABLE t_alias_capture_field;

-- An ALIAS named `k.a` is not the field of the parameter `k`.
DROP TABLE IF EXISTS t_alias_capture_dotted;
CREATE TABLE t_alias_capture_dotted
(
    z UInt32,
    `k.a` UInt32 ALIAS 1000,
    arr_t Array(Tuple(a UInt32)),
    m Array(UInt32) MATERIALIZED arrayMap(k -> k.a, arr_t)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_dotted (z, arr_t) VALUES (0, [tuple(100)]);
ALTER TABLE t_alias_capture_dotted UPDATE arr_t = [tuple(200)] WHERE 1;
SELECT 'dotted alias', m, arrayMap(k -> k.a, arr_t) FROM t_alias_capture_dotted;
DROP TABLE t_alias_capture_dotted;

-- A column TTL resets `k` in a merge, and the field of the parameter has to be read there too.
DROP TABLE IF EXISTS t_alias_capture_field_ttl;
CREATE TABLE t_alias_capture_field_ttl
(
    t DateTime,
    k UInt32 TTL t + INTERVAL 1 SECOND,
    arr_t Array(Tuple(a UInt32)),
    d UInt32 ALIAS k + 1,
    m Array(UInt32) MATERIALIZED arrayMap(k -> k.a + d, arr_t)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_alias_capture_field_ttl (t, k, arr_t) VALUES ('2000-01-01 00:00:00', 5, [tuple(100)]);
OPTIMIZE TABLE t_alias_capture_field_ttl FINAL;
SELECT 'field merge ttl', k, m, arrayMap(k -> k.a + d, arr_t) FROM t_alias_capture_field_ttl;
DROP TABLE t_alias_capture_field_ttl;

-- No column `k` with a field `a`.
DROP TABLE IF EXISTS t_alias_capture_field_scalar;
CREATE TABLE t_alias_capture_field_scalar
(
    k UInt32,
    arr_t Array(Tuple(a UInt32)),
    d UInt32 ALIAS k + 1,
    m Array(UInt32) MATERIALIZED arrayMap(k -> k.a + d, arr_t)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_field_scalar (k, arr_t) VALUES (5, [tuple(100)]);
ALTER TABLE t_alias_capture_field_scalar UPDATE arr_t = [tuple(200)] WHERE 1;
SELECT 'scalar update arr_t', m, arrayMap(k -> k.a + d, arr_t) FROM t_alias_capture_field_scalar;
ALTER TABLE t_alias_capture_field_scalar UPDATE k = 7 WHERE 1;
SELECT 'scalar update k', m, arrayMap(k -> k.a + d, arr_t) FROM t_alias_capture_field_scalar;
DROP TABLE t_alias_capture_field_scalar;

-- The field of a parameter no ALIAS reaches, including a nested one.
DROP TABLE IF EXISTS t_alias_capture_no_alias;
CREATE TABLE t_alias_capture_no_alias
(
    z UInt32,
    arr_t Array(Tuple(a UInt32)),
    arr_n Array(Tuple(a Tuple(b UInt32))),
    m Array(UInt32) MATERIALIZED arrayMap(x -> x.a, arr_t),
    m2 Array(UInt32) MATERIALIZED arrayMap(x -> x.a.b, arr_n)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_no_alias (z, arr_t, arr_n) VALUES (0, [tuple(100)], [tuple(tuple(7))]);
ALTER TABLE t_alias_capture_no_alias UPDATE z = 1 WHERE 1;
SELECT 'no alias update z', z, m, m2 FROM t_alias_capture_no_alias;
ALTER TABLE t_alias_capture_no_alias UPDATE arr_t = [tuple(200)], arr_n = [tuple(tuple(8))] WHERE 1;
SELECT 'no alias update arrays', m, m2, arrayMap(x -> x.a, arr_t), arrayMap(x -> x.a.b, arr_n) FROM t_alias_capture_no_alias;
DROP TABLE t_alias_capture_no_alias;

-- A field of a lambda parameter inside an ALIAS definition.
DROP TABLE IF EXISTS t_alias_capture_alias_field;
CREATE TABLE t_alias_capture_alias_field
(
    z UInt32,
    arr Array(UInt32),
    arr_t Array(Tuple(a UInt32)),
    d Array(UInt32) ALIAS arrayMap(x -> x.a, arr_t),
    m Array(UInt32) MATERIALIZED d,
    m2 Array(UInt32) MATERIALIZED arrayMap(x -> x + arraySum(d), arr)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_alias_field (z, arr, arr_t) VALUES (0, [1], [tuple(100)]);
ALTER TABLE t_alias_capture_alias_field UPDATE z = 1 WHERE 1;
SELECT 'alias field update z', z, m, m2, d, arrayMap(x -> x + arraySum(d), arr) FROM t_alias_capture_alias_field;
ALTER TABLE t_alias_capture_alias_field UPDATE arr_t = [tuple(200)] WHERE 1;
SELECT 'alias field update arr_t', m, m2, d, arrayMap(x -> x + arraySum(d), arr) FROM t_alias_capture_alias_field;
DROP TABLE t_alias_capture_alias_field;

-- A lambda parameter that shadows an ALIAS does not read it, also when the ALIAS reads a field of an inner alias (`tp.a`).
DROP TABLE IF EXISTS t_alias_capture_shadowed;
CREATE TABLE t_alias_capture_shadowed
(
    z UInt32,
    arr Array(UInt32),
    arr_t Array(Tuple(a UInt32)),
    d UInt32 ALIAS tupleElement(CAST(tuple(z + 1), 'Tuple(a UInt32)') AS tp, 'a') + tp.a,
    `k.a` UInt32 ALIAS tupleElement(CAST(tuple(z + 1000), 'Tuple(a UInt32)') AS tp2, 'a') + tp2.a,
    m Array(UInt32) MATERIALIZED arrayMap(d -> d + 1, arr),
    m_field Array(UInt32) MATERIALIZED arrayMap(k -> k.a, arr_t)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_shadowed (z, arr, arr_t) VALUES (0, [100], [tuple(100)]);
ALTER TABLE t_alias_capture_shadowed UPDATE z = 1 WHERE 1;
ALTER TABLE t_alias_capture_shadowed UPDATE arr = [200], arr_t = [tuple(200)] WHERE 1;
SELECT 'shadowed alias', z, d, m, m_field, arrayMap(d -> d + 1, arr), arrayMap(k -> k.a, arr_t) FROM t_alias_capture_shadowed;
DROP TABLE t_alias_capture_shadowed;

-- With named tuples, a parameter that no ALIAS definition reads keeps its name.
SET enable_named_columns_in_function_tuple = 1;
DROP TABLE IF EXISTS t_alias_capture_named_tuple SYNC;
CREATE TABLE t_alias_capture_named_tuple
(
    arr Array(UInt32),
    d2 Array(UInt32) ALIAS arrayMap(x -> x + 1, arr),
    m Array(Tuple(x UInt32, d2 Array(UInt32))) MATERIALIZED arrayMap(x -> tuple(x, d2), arr),
    m_names Array(Array(String)) MATERIALIZED arrayMap(x -> tupleNames(tuple(x, d2)), arr)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_named_tuple (arr) VALUES ([1]);
SYSTEM STOP MERGES t_alias_capture_named_tuple;
ALTER TABLE t_alias_capture_named_tuple UPDATE arr = [2] WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'named tuple on fly', m, m_names, arrayMap(x -> tuple(x, d2), arr), arrayMap(x -> tupleNames(tuple(x, d2)), arr)
FROM t_alias_capture_named_tuple SETTINGS apply_mutations_on_fly = 1;
SYSTEM START MERGES t_alias_capture_named_tuple;
DROP TABLE t_alias_capture_named_tuple SYNC;

-- An alias declared inside an ALIAS definition, or an ALIAS named `p.a`, does not rename a parameter.
DROP TABLE IF EXISTS t_alias_capture_alias_names SYNC;
CREATE TABLE t_alias_capture_alias_names
(
    k UInt32,
    arr Array(UInt32),
    d UInt32 ALIAS (k + 1 AS x),
    `p.a` UInt32 ALIAS 1000,
    e UInt32 ALIAS p.a + 1,
    m_root Array(Array(String)) MATERIALIZED arrayMap(x -> tupleNames(tuple(x, d)), arr)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_capture_alias_names (k, arr) SELECT 5, [1];
ALTER TABLE t_alias_capture_alias_names ADD COLUMN m_dotted Array(Array(String)) MATERIALIZED arrayMap(p -> tupleNames(tuple(p, e)), arr);
ALTER TABLE t_alias_capture_alias_names UPDATE arr = [2] WHERE 1 SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_alias_capture_alias_names;
ALTER TABLE t_alias_capture_alias_names UPDATE arr = [3] WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'alias names on fly', m_root, m_dotted, arrayMap(x -> tupleNames(tuple(x, d)), arr), arrayMap(p -> tupleNames(tuple(p, e)), arr)
FROM t_alias_capture_alias_names SETTINGS apply_mutations_on_fly = 1;
SYSTEM START MERGES t_alias_capture_alias_names;
DROP TABLE t_alias_capture_alias_names SYNC;
