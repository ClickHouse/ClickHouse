-- Row policies are attached to the parent table, but the `mergeTreeProjection` table function used to
-- read projection parts directly without applying them, leaking every row (clickhouse-private#53773).
-- The read must honour the parent table's row policy: apply it when the projection stores every column
-- the policy references, otherwise refuse the read.

DROP TABLE IF EXISTS users_rls_proj;
DROP ROW POLICY IF EXISTS rp_users_rls_proj ON users_rls_proj;

CREATE TABLE users_rls_proj (id UInt64, name String, department String, salary UInt64)
ENGINE = MergeTree ORDER BY id;

INSERT INTO users_rls_proj VALUES (1, 'Alice', 'engineering', 100000), (2, 'Bob', 'finance', 120000), (3, 'Carol', 'engineering', 110000), (4, 'Dave', 'hr', 90000);

-- proj_with_dept stores the policy column `department` (its ORDER BY key); proj_no_dept does not.
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

-- A policy on an ALIAS column is enforced by expanding it to the physical dependency the projection
-- stores (`c ALIAS b + 1` -> the filter reads `b`). The projection selects `b`, not the alias itself
-- (a projection cannot select an ALIAS column - it is not in the data block to materialize).
DROP TABLE IF EXISTS alias_rls_proj;
DROP ROW POLICY IF EXISTS rp_alias_rls_proj ON alias_rls_proj;

CREATE TABLE alias_rls_proj (a UInt64, b UInt64, c UInt64 ALIAS b + 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO alias_rls_proj (a, b) VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE alias_rls_proj ADD PROJECTION p (SELECT a, b ORDER BY a);
ALTER TABLE alias_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_alias_rls_proj ON alias_rls_proj FOR SELECT USING c > 21 TO ALL;

SELECT '-- policy on an ALIAS column is enforced via its physical dependency (expect a=3)';
SELECT a FROM mergeTreeProjection(currentDatabase(), 'alias_rls_proj', 'p') ORDER BY a;

DROP ROW POLICY rp_alias_rls_proj ON alias_rls_proj;
DROP TABLE alias_rls_proj;

-- A policy on a DEFAULT column uses the stored value, not the default expression: the projection stores
-- the column, so an explicitly inserted value (c = 999, not b + 1) is filtered on directly.
DROP TABLE IF EXISTS default_rls_proj;
DROP ROW POLICY IF EXISTS rp_default_rls_proj ON default_rls_proj;

CREATE TABLE default_rls_proj (a UInt64, b UInt64, c UInt64 DEFAULT b + 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO default_rls_proj (a, b) VALUES (1, 10), (2, 20);
INSERT INTO default_rls_proj (a, b, c) VALUES (3, 30, 999);

ALTER TABLE default_rls_proj ADD PROJECTION p (SELECT a, c ORDER BY a);
ALTER TABLE default_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_default_rls_proj ON default_rls_proj FOR SELECT USING c < 100 TO ALL;

SELECT '-- policy on a DEFAULT column filters the stored value, not b + 1 (expect 1, 2)';
SELECT a FROM mergeTreeProjection(currentDatabase(), 'default_rls_proj', 'p') ORDER BY a;

DROP ROW POLICY rp_default_rls_proj ON default_rls_proj;
DROP TABLE default_rls_proj;

-- A user PREWHERE must not observe rows the policy hides: the policy filter runs first, so throwIf
-- never sees the hidden 'private' row.
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

-- A policy on `_part_offset` is enforced when the projection preserves the parent offset (it selects
-- `_part_offset`, stored as `_parent_part_offset`), so parent-row semantics are kept.
DROP TABLE IF EXISTS virt_ok_rls_proj;
DROP ROW POLICY IF EXISTS rp_virt_ok_rls_proj ON virt_ok_rls_proj;

CREATE TABLE virt_ok_rls_proj (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO virt_ok_rls_proj VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE virt_ok_rls_proj ADD PROJECTION p (SELECT _part_offset, id, val ORDER BY val);
ALTER TABLE virt_ok_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_virt_ok_rls_proj ON virt_ok_rls_proj FOR SELECT USING _part_offset < 1 TO ALL;

SELECT '-- policy on _part_offset enforced when the projection preserves the parent offset (expect id=1)';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'virt_ok_rls_proj', 'p') ORDER BY id;

DROP ROW POLICY rp_virt_ok_rls_proj ON virt_ok_rls_proj;
DROP TABLE virt_ok_rls_proj;

-- A policy on `_part_offset` fails closed when the projection does not preserve the parent offset:
-- the projection's own offset means different rows, so it cannot be enforced (clean ACCESS_DENIED).
DROP TABLE IF EXISTS virt_rls_proj;
DROP ROW POLICY IF EXISTS rp_virt_rls_proj ON virt_rls_proj;

CREATE TABLE virt_rls_proj (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO virt_rls_proj VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE virt_rls_proj ADD PROJECTION p (SELECT id, val ORDER BY val);
ALTER TABLE virt_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_virt_rls_proj ON virt_rls_proj FOR SELECT USING _part_offset < 1 TO ALL;

SELECT '-- policy on _part_offset without a preserved parent offset: read is refused';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'virt_rls_proj', 'p') ORDER BY id; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_virt_rls_proj ON virt_rls_proj;
DROP TABLE virt_rls_proj;

-- A bare-column policy (`USING flag`, where the predicate result is an existing column, not a new node).
DROP TABLE IF EXISTS bare_rls_proj;
DROP ROW POLICY IF EXISTS rp_bare_rls_proj ON bare_rls_proj;

CREATE TABLE bare_rls_proj (id UInt64, visible UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO bare_rls_proj VALUES (1, 1), (2, 0), (3, 1);

ALTER TABLE bare_rls_proj ADD PROJECTION p (SELECT id, visible ORDER BY id);
ALTER TABLE bare_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_bare_rls_proj ON bare_rls_proj FOR SELECT USING visible TO ALL;

SELECT '-- bare-column policy is enforced when the filter column is not selected (expect 1, 3)';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'bare_rls_proj', 'p') ORDER BY id;

SELECT '-- bare-column policy is enforced when the filter column is also selected (expect 1, 3)';
SELECT id, visible FROM mergeTreeProjection(currentDatabase(), 'bare_rls_proj', 'p') ORDER BY id;

DROP ROW POLICY rp_bare_rls_proj ON bare_rls_proj;
DROP TABLE bare_rls_proj;

-- A policy on `_part_starting_offset + _part_offset` (the global row index) is enforced: the projection
-- preserves both the parent offset and the part starting offset.
DROP TABLE IF EXISTS pso_rls_proj;
DROP ROW POLICY IF EXISTS rp_pso_rls_proj ON pso_rls_proj;

CREATE TABLE pso_rls_proj (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO pso_rls_proj VALUES (1, 10), (2, 20);
INSERT INTO pso_rls_proj VALUES (3, 30), (4, 40);

ALTER TABLE pso_rls_proj ADD PROJECTION p (SELECT _part_offset, id, val ORDER BY val);
ALTER TABLE pso_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_pso_rls_proj ON pso_rls_proj FOR SELECT USING _part_starting_offset + _part_offset < 3 TO ALL;

SELECT '-- policy on global row index (_part_starting_offset + _part_offset < 3) enforced (expect 1, 2, 3)';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'pso_rls_proj', 'p') ORDER BY id;

DROP ROW POLICY rp_pso_rls_proj ON pso_rls_proj;
DROP TABLE pso_rls_proj;

-- A policy on a part-identity virtual (`_partition_id`) is enforced: a projection part maps back to its
-- parent part, so the virtual keeps parent-row semantics (unlike the projection-local `_part_offset`).
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
