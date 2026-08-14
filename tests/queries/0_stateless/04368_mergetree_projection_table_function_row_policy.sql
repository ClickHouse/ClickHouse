-- Tags: no-parallel
-- ^ the UDF case creates a global SQL UDF (CREATE FUNCTION), which cannot run concurrently.
-- Row policies live on the parent table; mergeTreeProjection used to read projection parts without
-- applying them (clickhouse-private#53773). The read now resolves the policy against the projection's
-- own columns with the analyzer and refuses when it cannot be enforced there.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS users_rls_proj;
DROP ROW POLICY IF EXISTS rp_users_rls_proj ON users_rls_proj;

CREATE TABLE users_rls_proj (id UInt64, name String, department String, salary UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO users_rls_proj VALUES (1, 'Alice', 'engineering', 100000), (2, 'Bob', 'finance', 120000), (3, 'Carol', 'engineering', 110000), (4, 'Dave', 'hr', 90000);

-- proj_with_dept stores the policy column `department`; proj_no_dept does not.
ALTER TABLE users_rls_proj ADD PROJECTION proj_with_dept (SELECT id, name, salary ORDER BY department);
ALTER TABLE users_rls_proj ADD PROJECTION proj_no_dept (SELECT id, name ORDER BY id);
ALTER TABLE users_rls_proj MATERIALIZE PROJECTION proj_with_dept SETTINGS mutations_sync = 2;
ALTER TABLE users_rls_proj MATERIALIZE PROJECTION proj_no_dept SETTINGS mutations_sync = 2;

SELECT '-- no policy: table function returns all rows';
SELECT name FROM mergeTreeProjection(currentDatabase(), 'users_rls_proj', 'proj_with_dept') ORDER BY name;

CREATE ROW POLICY rp_users_rls_proj ON users_rls_proj FOR SELECT USING department = 'engineering' TO ALL;

SELECT '-- base table honours the policy';
SELECT name FROM users_rls_proj ORDER BY name;

SELECT '-- projection stores the policy column: filter is applied';
SELECT name FROM mergeTreeProjection(currentDatabase(), 'users_rls_proj', 'proj_with_dept') ORDER BY name;

SELECT '-- filter applied even when the policy column is selected explicitly';
SELECT name, department FROM mergeTreeProjection(currentDatabase(), 'users_rls_proj', 'proj_with_dept') ORDER BY name;

SELECT '-- projection lacks the policy column: read is refused';
SELECT name FROM mergeTreeProjection(currentDatabase(), 'users_rls_proj', 'proj_no_dept') ORDER BY name; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_users_rls_proj ON users_rls_proj;
DROP TABLE users_rls_proj;

-- A bare-column policy (`USING flag`).
DROP TABLE IF EXISTS bare_rls_proj;
DROP ROW POLICY IF EXISTS rp_bare_rls_proj ON bare_rls_proj;

CREATE TABLE bare_rls_proj (id UInt64, visible UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO bare_rls_proj VALUES (1, 1), (2, 0), (3, 1);

ALTER TABLE bare_rls_proj ADD PROJECTION p (SELECT id, visible ORDER BY id);
ALTER TABLE bare_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_bare_rls_proj ON bare_rls_proj FOR SELECT USING visible TO ALL;

SELECT '-- bare-column policy is enforced (expect 1, 3)';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'bare_rls_proj', 'p') ORDER BY id;

DROP ROW POLICY rp_bare_rls_proj ON bare_rls_proj;
DROP TABLE bare_rls_proj;

-- A part-identity virtual (`_partition_id`) has the same value in the projection as in the parent.
DROP TABLE IF EXISTS pid_rls_proj;
DROP ROW POLICY IF EXISTS rp_pid_rls_proj ON pid_rls_proj;

CREATE TABLE pid_rls_proj (id UInt64, val UInt64) ENGINE = MergeTree PARTITION BY (id % 2) ORDER BY id;
INSERT INTO pid_rls_proj VALUES (1, 10), (2, 20), (3, 30), (4, 40);

ALTER TABLE pid_rls_proj ADD PROJECTION p (SELECT id, val ORDER BY val);
ALTER TABLE pid_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_pid_rls_proj ON pid_rls_proj FOR SELECT USING _partition_id = '1' TO ALL;

SELECT '-- policy on the _partition_id virtual is enforced (expect 1, 3)';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'pid_rls_proj', 'p') ORDER BY id;

DROP ROW POLICY rp_pid_rls_proj ON pid_rls_proj;
DROP TABLE pid_rls_proj;

-- A DEFAULT column stored in the projection is filtered on its stored value, not the default expression
-- (an explicitly inserted c = 999 stays hidden by `c < 100`).
DROP TABLE IF EXISTS default_rls_proj;
DROP ROW POLICY IF EXISTS rp_default_rls_proj ON default_rls_proj;

CREATE TABLE default_rls_proj (a UInt64, b UInt64, c UInt64 DEFAULT b + 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO default_rls_proj (a, b) VALUES (1, 10), (2, 20);
INSERT INTO default_rls_proj (a, b, c) VALUES (3, 30, 999);

ALTER TABLE default_rls_proj ADD PROJECTION p (SELECT a, c ORDER BY a);
ALTER TABLE default_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_default_rls_proj ON default_rls_proj FOR SELECT USING c < 100 TO ALL;

SELECT '-- DEFAULT column stored: filtered on the stored value (expect 1, 2)';
SELECT a FROM mergeTreeProjection(currentDatabase(), 'default_rls_proj', 'p') ORDER BY a;

DROP ROW POLICY rp_default_rls_proj ON default_rls_proj;
DROP TABLE default_rls_proj;

-- A user PREWHERE must not observe rows the policy hides: the policy filter runs first.
DROP TABLE IF EXISTS prewhere_rls_proj;
DROP ROW POLICY IF EXISTS rp_prewhere_rls_proj ON prewhere_rls_proj;

CREATE TABLE prewhere_rls_proj (id UInt64, secret String, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO prewhere_rls_proj VALUES (1, 'public', 10), (2, 'private', 20), (3, 'public', 30);

ALTER TABLE prewhere_rls_proj ADD PROJECTION p (SELECT id, secret, val ORDER BY secret);
ALTER TABLE prewhere_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_prewhere_rls_proj ON prewhere_rls_proj FOR SELECT USING secret = 'public' TO ALL;

SELECT '-- user PREWHERE does not observe policy-hidden rows (expect 1, 3)';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'prewhere_rls_proj', 'p')
PREWHERE throwIf(secret = 'private', 'row policy leak') = 0 ORDER BY id;

DROP ROW POLICY rp_prewhere_rls_proj ON prewhere_rls_proj;
DROP TABLE prewhere_rls_proj;

-- A LIMIT must not stop the read before the policy filters (hiding the first 50 rows, asking for 1).
DROP TABLE IF EXISTS limit_rls_proj;
DROP ROW POLICY IF EXISTS rp_limit_rls_proj ON limit_rls_proj;

CREATE TABLE limit_rls_proj (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO limit_rls_proj SELECT number FROM numbers(1, 100);

ALTER TABLE limit_rls_proj ADD PROJECTION p (SELECT id ORDER BY id);
ALTER TABLE limit_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_limit_rls_proj ON limit_rls_proj FOR SELECT USING id > 50 TO ALL;

SELECT '-- LIMIT returns visible rows, not rows hidden by the policy (expect 1)';
SELECT count() FROM (SELECT id FROM mergeTreeProjection(currentDatabase(), 'limit_rls_proj', 'p') LIMIT 1);

DROP ROW POLICY rp_limit_rls_proj ON limit_rls_proj;
DROP TABLE limit_rls_proj;

-- ORDER BY the projection sort key + LIMIT (read-in-order): the in-order limit is a soft output limit,
-- so the read must skip the hidden leading rows and return the first visible one (expect 51, not empty).
DROP TABLE IF EXISTS order_limit_rls_proj;
DROP ROW POLICY IF EXISTS rp_order_limit_rls_proj ON order_limit_rls_proj;

CREATE TABLE order_limit_rls_proj (id UInt64, visible UInt8) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO order_limit_rls_proj SELECT number + 1, number >= 50 FROM numbers(200);

ALTER TABLE order_limit_rls_proj ADD PROJECTION p (SELECT id, visible ORDER BY id);
ALTER TABLE order_limit_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_order_limit_rls_proj ON order_limit_rls_proj FOR SELECT USING visible TO ALL;

SELECT '-- read-in-order + LIMIT skips the policy-hidden leading rows (expect 51)';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'order_limit_rls_proj', 'p') ORDER BY id LIMIT 1
SETTINGS optimize_read_in_order = 1;

DROP ROW POLICY rp_order_limit_rls_proj ON order_limit_rls_proj;
DROP TABLE order_limit_rls_proj;

-- The remaining cases cannot be enforced against the projection, so the read is refused.

-- ALIAS column: the projection stores the dependency `b`, not the alias `c`, and the analyzer resolves
-- the policy against the projection, where `c` is unknown.
DROP TABLE IF EXISTS alias_rls_proj;
DROP ROW POLICY IF EXISTS rp_alias_rls_proj ON alias_rls_proj;

CREATE TABLE alias_rls_proj (a UInt64, b UInt64, c UInt64 ALIAS b + 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO alias_rls_proj (a, b) VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE alias_rls_proj ADD PROJECTION p (SELECT a, b ORDER BY a);
ALTER TABLE alias_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_alias_rls_proj ON alias_rls_proj FOR SELECT USING c > 21 TO ALL;

SELECT '-- policy on an ALIAS column: read is refused';
SELECT a FROM mergeTreeProjection(currentDatabase(), 'alias_rls_proj', 'p') ORDER BY a; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_alias_rls_proj ON alias_rls_proj;
DROP TABLE alias_rls_proj;

-- DEFAULT column with only its dependency stored: the stored value can differ from `b + 1`
-- (an explicit c = 5), so it cannot be reconstructed from `b` - refused.
DROP TABLE IF EXISTS default_dep_rls_proj;
DROP ROW POLICY IF EXISTS rp_default_dep_rls_proj ON default_dep_rls_proj;

CREATE TABLE default_dep_rls_proj (a UInt64, b UInt64, c UInt64 DEFAULT b + 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO default_dep_rls_proj (a, b) VALUES (1, 10), (2, 20);
INSERT INTO default_dep_rls_proj (a, b, c) VALUES (3, 30, 5);

ALTER TABLE default_dep_rls_proj ADD PROJECTION p (SELECT a, b ORDER BY a);
ALTER TABLE default_dep_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_default_dep_rls_proj ON default_dep_rls_proj FOR SELECT USING c > 20 TO ALL;

SELECT '-- policy on a DEFAULT column with only its dependency stored: read is refused';
SELECT a FROM mergeTreeProjection(currentDatabase(), 'default_dep_rls_proj', 'p') ORDER BY a; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_default_dep_rls_proj ON default_dep_rls_proj;
DROP TABLE default_dep_rls_proj;

-- MATERIALIZED column with only its dependency stored: the stored value can diverge from the current
-- expression (e.g. after ALTER MODIFY, which does not re-materialize old parts) - refused.
DROP TABLE IF EXISTS materialized_dep_rls_proj;
DROP ROW POLICY IF EXISTS rp_materialized_dep_rls_proj ON materialized_dep_rls_proj;

CREATE TABLE materialized_dep_rls_proj (x UInt64, m UInt64 MATERIALIZED x + 1) ENGINE = MergeTree ORDER BY x;
INSERT INTO materialized_dep_rls_proj (x) VALUES (1), (2), (3);

ALTER TABLE materialized_dep_rls_proj ADD PROJECTION p (SELECT x ORDER BY x);
ALTER TABLE materialized_dep_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_materialized_dep_rls_proj ON materialized_dep_rls_proj FOR SELECT USING m > 2 TO ALL;

SELECT '-- policy on a MATERIALIZED column with only its dependency stored: read is refused';
SELECT x FROM mergeTreeProjection(currentDatabase(), 'materialized_dep_rls_proj', 'p') ORDER BY x; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_materialized_dep_rls_proj ON materialized_dep_rls_proj;
DROP TABLE materialized_dep_rls_proj;

-- A position-relative virtual (`_part_offset`): the projection can reorder rows, so its value is not the
-- parent's - refused, both directly and when hidden inside a SQL UDF.
DROP TABLE IF EXISTS virt_rls_proj;
DROP ROW POLICY IF EXISTS rp_virt_rls_proj ON virt_rls_proj;
DROP FUNCTION IF EXISTS rp_visible_04368;

CREATE TABLE virt_rls_proj (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO virt_rls_proj VALUES (1, 30), (2, 20), (3, 10);

ALTER TABLE virt_rls_proj ADD PROJECTION p (SELECT _part_offset, id, val ORDER BY val);
ALTER TABLE virt_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_virt_rls_proj ON virt_rls_proj FOR SELECT USING _part_offset < 1 TO ALL;

SELECT '-- policy on a position-relative virtual (_part_offset): read is refused';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'virt_rls_proj', 'p') ORDER BY id; -- { serverError ACCESS_DENIED }

CREATE FUNCTION rp_visible_04368 AS (x) -> x < 1;
DROP ROW POLICY rp_virt_rls_proj ON virt_rls_proj;
CREATE ROW POLICY rp_virt_rls_proj ON virt_rls_proj FOR SELECT USING rp_visible_04368(_part_offset) TO ALL;

SELECT '-- same position-relative virtual wrapped in a SQL UDF: read is refused';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'virt_rls_proj', 'p') ORDER BY id; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_virt_rls_proj ON virt_rls_proj;
DROP FUNCTION rp_visible_04368;
DROP TABLE virt_rls_proj;

-- A projection expression bound to a parent column name is not exposed under that name, so the policy
-- cannot resolve it - refused (never filters on the transformed value).
DROP TABLE IF EXISTS shadow_rls_proj;
DROP ROW POLICY IF EXISTS rp_shadow_rls_proj ON shadow_rls_proj;

CREATE TABLE shadow_rls_proj (a UInt8, c UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO shadow_rls_proj VALUES (1, 0), (2, 1), (3, 0);

ALTER TABLE shadow_rls_proj ADD PROJECTION p (SELECT a, (a = 1) AS c ORDER BY a);
ALTER TABLE shadow_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_shadow_rls_proj ON shadow_rls_proj FOR SELECT USING c TO ALL;

SELECT '-- policy on a column shadowed by a projection expression: read is refused';
SELECT a FROM mergeTreeProjection(currentDatabase(), 'shadow_rls_proj', 'p') ORDER BY a; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_shadow_rls_proj ON shadow_rls_proj;
DROP TABLE shadow_rls_proj;

-- An aggregate projection stores states built from many parent rows, so a per-row policy cannot be
-- enforced after aggregation - refused, even when the policy only touches the GROUP BY key.
DROP TABLE IF EXISTS agg_rls_proj;
DROP ROW POLICY IF EXISTS rp_agg_rls_proj ON agg_rls_proj;

CREATE TABLE agg_rls_proj (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO agg_rls_proj VALUES (1, 10), (1, 20), (2, 30);

ALTER TABLE agg_rls_proj ADD PROJECTION p (SELECT key, sum(value) GROUP BY key);
ALTER TABLE agg_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_agg_rls_proj ON agg_rls_proj FOR SELECT USING key = 1 TO ALL;

SELECT '-- aggregate projection under a row policy: read is refused';
SELECT key FROM mergeTreeProjection(currentDatabase(), 'agg_rls_proj', 'p'); -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_agg_rls_proj ON agg_rls_proj;
DROP TABLE agg_rls_proj;

-- The _block_number virtual is not preserved by the projection (synthesized from the parent part), so
-- a policy on it cannot be enforced - refused.
DROP TABLE IF EXISTS blocknum_rls_proj;
DROP ROW POLICY IF EXISTS rp_blocknum_rls_proj ON blocknum_rls_proj;

CREATE TABLE blocknum_rls_proj (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO blocknum_rls_proj VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE blocknum_rls_proj ADD PROJECTION p (SELECT id, val ORDER BY val);
ALTER TABLE blocknum_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_blocknum_rls_proj ON blocknum_rls_proj FOR SELECT USING _block_number = 1 TO ALL;

SELECT '-- policy on the _block_number virtual: read is refused';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'blocknum_rls_proj', 'p') ORDER BY id; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_blocknum_rls_proj ON blocknum_rls_proj;
DROP TABLE blocknum_rls_proj;

-- _parent_part_offset is a projection-only name absent on the parent, so a parent policy binding to it
-- would diverge from the parent (where the read errors) - refused.
DROP TABLE IF EXISTS pparent_rls_proj;
DROP ROW POLICY IF EXISTS rp_pparent_rls_proj ON pparent_rls_proj;

CREATE TABLE pparent_rls_proj (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO pparent_rls_proj VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE pparent_rls_proj ADD PROJECTION p (SELECT _part_offset, id, val ORDER BY val);
ALTER TABLE pparent_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_pparent_rls_proj ON pparent_rls_proj FOR SELECT USING _parent_part_offset = 0 TO ALL;

SELECT '-- policy on the projection-only _parent_part_offset name: read is refused';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'pparent_rls_proj', 'p') ORDER BY id; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_pparent_rls_proj ON pparent_rls_proj;
DROP TABLE pparent_rls_proj;

-- A row policy on the table function itself (_table_function.*) must not be dropped by the parent policy;
-- the two cannot be combined here, so the read is refused rather than losing the table-function restriction.
DROP TABLE IF EXISTS tf_rls_proj;
DROP ROW POLICY IF EXISTS rp_tf_rls_proj ON _table_function.*;
DROP ROW POLICY IF EXISTS rp_tf_parent_rls_proj ON tf_rls_proj;

CREATE TABLE tf_rls_proj (id UInt64, dept String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tf_rls_proj VALUES (1, 'eng'), (2, 'fin'), (3, 'eng');

ALTER TABLE tf_rls_proj ADD PROJECTION p (SELECT id, dept ORDER BY dept);
ALTER TABLE tf_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_tf_rls_proj ON _table_function.* FOR SELECT USING 0 TO ALL;
CREATE ROW POLICY rp_tf_parent_rls_proj ON tf_rls_proj FOR SELECT USING dept = 'eng' TO ALL;

SELECT '-- table-function row policy combined with a parent policy: read is refused';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'tf_rls_proj', 'p') ORDER BY id; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_tf_rls_proj ON _table_function.*;
DROP ROW POLICY rp_tf_parent_rls_proj ON tf_rls_proj;
DROP TABLE tf_rls_proj;
