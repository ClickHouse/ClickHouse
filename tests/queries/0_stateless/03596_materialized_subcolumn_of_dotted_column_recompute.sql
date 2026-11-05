-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation this test needs to stay pending

-- A MATERIALIZED column reading a subcolumn of a column whose name has a dot is recomputed when that column changes.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_alter;
CREATE TABLE t_alter
(
    `t.x` Tuple(a UInt32),
    m UInt32 MATERIALIZED t.x.a * 10,
    m2 String MATERIALIZED concat(toString(t.x.a), '-', formatRowNoNewline('JSONEachRow', t.x.a))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alter (`t.x`) VALUES (tuple(1));
ALTER TABLE t_alter UPDATE `t.x` = tuple(2) WHERE 1;
SELECT 'alter update', `t.x`, m, m2 FROM t_alter;
DROP TABLE t_alter;

DROP TABLE IF EXISTS t_lightweight;
CREATE TABLE t_lightweight (`t.x` Tuple(a UInt32), m UInt32 MATERIALIZED t.x.a * 10) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lightweight (`t.x`) VALUES (tuple(1));
SET enable_lightweight_update = 1;
UPDATE t_lightweight SET `t.x` = tuple(2) WHERE 1;
SELECT 'lightweight update', `t.x`, m FROM t_lightweight;
OPTIMIZE TABLE t_lightweight FINAL;
SELECT 'lightweight update, merged', `t.x`, m FROM t_lightweight;
DROP TABLE t_lightweight;

DROP TABLE IF EXISTS t_ttl;
CREATE TABLE t_ttl (d DateTime, `t.x` Tuple(a UInt32) TTL d + INTERVAL 1 SECOND, m UInt32 MATERIALIZED t.x.a * 10)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ttl (d, `t.x`) VALUES ('2000-01-01 00:00:00', tuple(7));
OPTIMIZE TABLE t_ttl FINAL;
SELECT 'column ttl', `t.x`, m FROM t_ttl;
DROP TABLE t_ttl;

DROP TABLE IF EXISTS t_materialize_ttl;
CREATE TABLE t_materialize_ttl (d DateTime, `t.x` Tuple(a UInt32), m UInt32 MATERIALIZED t.x.a * 10)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0;
INSERT INTO t_materialize_ttl (d, `t.x`) VALUES ('2000-01-01 00:00:00', tuple(7));
ALTER TABLE t_materialize_ttl MODIFY COLUMN `t.x` Tuple(a UInt32) TTL d + INTERVAL 1 SECOND SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_materialize_ttl MATERIALIZE TTL;
SELECT 'materialize ttl', `t.x`, m FROM t_materialize_ttl;
DROP TABLE t_materialize_ttl;

DROP TABLE IF EXISTS t_on_fly;
CREATE TABLE t_on_fly (`t.x` Tuple(a UInt32), m UInt32 MATERIALIZED t.x.a * 10) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_on_fly (`t.x`) VALUES (tuple(1));
SYSTEM STOP MERGES t_on_fly;
ALTER TABLE t_on_fly UPDATE `t.x` = tuple(2) WHERE 1 SETTINGS mutations_sync = 0;
SELECT 'on the fly', `t.x`, m FROM t_on_fly SETTINGS apply_mutations_on_fly = 1;
DROP TABLE t_on_fly;

DROP TABLE IF EXISTS t_key;
CREATE TABLE t_key (`t.x` Tuple(a UInt32), m UInt32 MATERIALIZED t.x.a * 10) ENGINE = MergeTree ORDER BY m;
INSERT INTO t_key (`t.x`) VALUES (tuple(1));
ALTER TABLE t_key UPDATE `t.x` = tuple(2) WHERE 1; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_key;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (n Nested(a UInt32), m UInt64 MATERIALIZED n.a.size0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested (`n.a`) VALUES ([1, 2, 3]);
ALTER TABLE t_nested UPDATE `n.a` = [7] WHERE 1;
SELECT 'nested member', `n.a`, m FROM t_nested;
DROP TABLE t_nested;

DROP TABLE IF EXISTS t_nested_tuple;
CREATE TABLE t_nested_tuple (n Nested(t Tuple(a UInt32)), m Array(UInt32) MATERIALIZED n.t.a) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested_tuple (`n.t`) VALUES ([tuple(1)]);
ALTER TABLE t_nested_tuple UPDATE `n.t` = [tuple(5)] WHERE 1;
SELECT 'nested tuple member', `n.t`, m FROM t_nested_tuple;
DROP TABLE t_nested_tuple;

DROP TABLE IF EXISTS t_json;
CREATE TABLE t_json (`j.k` JSON, m UInt32 MATERIALIZED j.k.a::UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json (`j.k`) VALUES ('{"a":1}');
ALTER TABLE t_json UPDATE `j.k` = '{"a":2}' WHERE 1;
SELECT 'json', `j.k`, m FROM t_json;
DROP TABLE t_json;

DROP TABLE IF EXISTS t_ephemeral;
CREATE TABLE t_ephemeral (`e.x` Tuple(a UInt32) EPHEMERAL, y UInt32, m UInt32 MATERIALIZED e.x.a + y) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ephemeral (`e.x`, y) VALUES (tuple(5), 1);
-- `m` reads an EPHEMERAL column, so it is not recomputed and the mutation logs a warning.
ALTER TABLE t_ephemeral UPDATE y = 2 WHERE 1 SETTINGS send_logs_level = 'error';
SELECT 'ephemeral', y, m FROM t_ephemeral;
DROP TABLE t_ephemeral;

DROP TABLE IF EXISTS t_lambda_parameter;
CREATE TABLE t_lambda_parameter
(
    `p.f` Tuple(g UInt32),
    arr Array(Tuple(f Tuple(g UInt32))),
    m Array(UInt32) MATERIALIZED arrayMap(p -> p.f.g, arr)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_parameter (`p.f`, arr) VALUES (tuple(999), [tuple(tuple(1))]);
ALTER TABLE t_lambda_parameter UPDATE arr = [tuple(tuple(2))] WHERE 1;
SELECT 'lambda parameter', m FROM t_lambda_parameter;
DROP TABLE t_lambda_parameter;

DROP TABLE IF EXISTS t_lambda_body;
CREATE TABLE t_lambda_body (`t.x` Tuple(a UInt32), arr Array(UInt32), m Array(UInt32) MATERIALIZED arrayMap(v -> v + t.x.a, arr))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_body (`t.x`, arr) VALUES (tuple(10), [1, 2]);
ALTER TABLE t_lambda_body UPDATE `t.x` = tuple(20) WHERE 1;
SELECT 'lambda body', m FROM t_lambda_body;
DROP TABLE t_lambda_body;

DROP TABLE IF EXISTS t_alias;
CREATE TABLE t_alias (`t.x` Tuple(a UInt32), y UInt32, m UInt32 MATERIALIZED (t.x.a AS r) + r + y) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias (`t.x`, y) VALUES (tuple(1), 1);
ALTER TABLE t_alias UPDATE y = 2 WHERE 1;
SELECT 'alias, y updated', m FROM t_alias;
ALTER TABLE t_alias UPDATE `t.x` = tuple(2) WHERE 1;
SELECT 'alias, t.x updated', m FROM t_alias;
DROP TABLE t_alias;

DROP TABLE IF EXISTS t_name;
CREATE TABLE t_name
(
    `t.x` Tuple(a UInt32),
    y UInt32,
    m String MATERIALIZED concat(formatRowNoNewline('JSONEachRow', t.x.a), '-', toString(y))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_name (`t.x`, y) VALUES (tuple(1), 1);
ALTER TABLE t_name UPDATE `t.x` = tuple(2) WHERE 1;
SELECT 'name, t.x updated', m FROM t_name;
ALTER TABLE t_name UPDATE y = 2 WHERE 1;
SELECT 'name, y updated', m FROM t_name;
DROP TABLE t_name;

DROP TABLE IF EXISTS t_alias_prefix;
CREATE TABLE t_alias_prefix
(
    `a.f` Tuple(g UInt32),
    arr Array(Tuple(f Tuple(g UInt32))),
    m Array(UInt32) MATERIALIZED arrayMap(z -> ((z AS a).f.g + 0, a.f.g).2, arr)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_prefix (`a.f`, arr) VALUES (tuple(1000), [tuple(tuple(5))]);
ALTER TABLE t_alias_prefix UPDATE arr = [tuple(tuple(6))] WHERE 1;
SELECT 'alias prefix', m FROM t_alias_prefix;
DROP TABLE t_alias_prefix;

DROP TABLE IF EXISTS t_masked_name;
CREATE TABLE t_masked_name
(
    `t.x` Tuple(a UInt32),
    arr Array(Tuple(x Tuple(a UInt32))),
    m String MATERIALIZED concat(formatRowNoNewline('JSONEachRow', t.x.a), '-', toString(arraySum(arrayMap(t -> t.x.a, arr))))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_masked_name (`t.x`, arr) VALUES (tuple(7), [tuple(tuple(1))]);
SELECT 'masked name, inserted', m FROM t_masked_name;
ALTER TABLE t_masked_name UPDATE arr = [tuple(tuple(2))] WHERE 1;
SELECT 'masked name, arr updated', m FROM t_masked_name;
DROP TABLE t_masked_name;

DROP TABLE IF EXISTS t_self_alias;
CREATE TABLE t_self_alias (`t.x` Tuple(a UInt32), m UInt32 MATERIALIZED (t.x.a AS t) + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_self_alias (`t.x`) VALUES (tuple(1));
ALTER TABLE t_self_alias UPDATE `t.x` = tuple(2) WHERE 1;
SELECT 'self alias', m FROM t_self_alias;
DROP TABLE t_self_alias;

DROP TABLE IF EXISTS t_full_name_alias;
CREATE TABLE t_full_name_alias
(
    `t.x` Tuple(a UInt32),
    y UInt32,
    m String MATERIALIZED concat(toString(y * 10 AS `t.x.a`), '-', formatRowNoNewline('JSONEachRow', t.x.a))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_full_name_alias (`t.x`, y) VALUES (tuple(1), 1);
SELECT 'full name alias, inserted', m FROM t_full_name_alias;
ALTER TABLE t_full_name_alias UPDATE y = 2 WHERE 1;
SELECT 'full name alias, y updated', m FROM t_full_name_alias;
DROP TABLE t_full_name_alias;

DROP TABLE IF EXISTS t_column_captured;
CREATE TABLE t_column_captured
(
    `t.x` Tuple(a UInt32),
    y UInt32,
    arr Array(UInt32),
    m1 UInt32 MATERIALIZED t.x.a + (CAST(tuple(y), 'Tuple(a UInt32)') AS `t.x`).a,
    m2 Array(UInt32) MATERIALIZED arrayMap(`t.x` -> t.x.a, arr),
    m3 Array(UInt32) MATERIALIZED arrayMap(`t.x.a` -> t.x.a + 1, arr)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_column_captured (`t.x`, y, arr) VALUES (tuple(1), 10, [10, 20]);
ALTER TABLE t_column_captured UPDATE y = 20, arr = [30] WHERE 1;
SELECT 'column captured', m1, m2, m3 FROM t_column_captured;
DROP TABLE t_column_captured;

DROP TABLE IF EXISTS t_leading_dot;
CREATE TABLE t_leading_dot
(
    `.t` Tuple(a UInt32),
    y UInt32,
    arr Array(UInt32),
    m1 String MATERIALIZED concat(formatRowNoNewline('JSONEachRow', `.t.a`), '-', toString(y)),
    m2 Array(UInt32) MATERIALIZED arrayMap(`.t.a` -> `.t.a` + 1, arr)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_leading_dot (`.t`, y, arr) VALUES (tuple(1), 1, [1]);
ALTER TABLE t_leading_dot UPDATE y = 2, arr = [5] WHERE 1;
SELECT 'leading dot, y updated', m1, m2 FROM t_leading_dot;
ALTER TABLE t_leading_dot UPDATE `.t` = tuple(3) WHERE 1;
SELECT 'leading dot, .t updated', m1, m2 FROM t_leading_dot;
DROP TABLE t_leading_dot;

DROP TABLE IF EXISTS t_own_alias_name;
CREATE TABLE t_own_alias_name (`t.x` Tuple(a UInt32), m UInt32 MATERIALIZED (t.x.a AS `t.x.a`) + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_own_alias_name (`t.x`) VALUES (tuple(1));
ALTER TABLE t_own_alias_name UPDATE `t.x` = tuple(2) WHERE 1;
SELECT 'own alias, name', `t.x`, m FROM t_own_alias_name;
DROP TABLE t_own_alias_name;

DROP TABLE IF EXISTS t_own_alias_column;
CREATE TABLE t_own_alias_column (`t.x` Tuple(a UInt32), m UInt32 MATERIALIZED (t.x.a AS `t.x`) + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_own_alias_column (`t.x`) VALUES (tuple(1));
ALTER TABLE t_own_alias_column UPDATE `t.x` = tuple(2) WHERE 1;
SELECT 'own alias, column', `t.x`, m FROM t_own_alias_column;
DROP TABLE t_own_alias_column;

DROP TABLE IF EXISTS t_enclosing_alias;
CREATE TABLE t_enclosing_alias (`t.x` Tuple(a UInt32), y UInt32, m UInt32 MATERIALIZED (((t.x.a AS z) + y) AS `t.x`) * 2 + z)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enclosing_alias (`t.x`, y) VALUES (tuple(1), 1);
ALTER TABLE t_enclosing_alias UPDATE y = 2 WHERE 1;
SELECT 'enclosing alias, y updated', m FROM t_enclosing_alias;
DROP TABLE t_enclosing_alias;
