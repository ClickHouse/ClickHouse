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

-- A policy on an ALIAS column fails closed: the projection stores the alias's physical dependency
-- under a different name and does not expose the alias, so the policy cannot be evaluated here.
DROP TABLE IF EXISTS alias_rls_proj;
DROP ROW POLICY IF EXISTS rp_alias_rls_proj ON alias_rls_proj;

CREATE TABLE alias_rls_proj (a UInt64, b UInt64, c UInt64 ALIAS b + 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO alias_rls_proj (a, b) VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE alias_rls_proj ADD PROJECTION p (SELECT a, c ORDER BY a);
ALTER TABLE alias_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_alias_rls_proj ON alias_rls_proj FOR SELECT USING c > 21 TO ALL;

SELECT '-- policy on an ALIAS column: read is refused';
SELECT a FROM mergeTreeProjection(currentDatabase(), 'alias_rls_proj', 'p') ORDER BY a; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_alias_rls_proj ON alias_rls_proj;
DROP TABLE alias_rls_proj;

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

-- A policy on a virtual column fails closed: virtuals like `_part_offset` mean different rows on the
-- reordered projection, so they cannot be enforced there (a clean ACCESS_DENIED, not UNKNOWN_IDENTIFIER).
DROP TABLE IF EXISTS virt_rls_proj;
DROP ROW POLICY IF EXISTS rp_virt_rls_proj ON virt_rls_proj;

CREATE TABLE virt_rls_proj (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO virt_rls_proj VALUES (1, 10), (2, 20), (3, 30);

ALTER TABLE virt_rls_proj ADD PROJECTION p (SELECT id, val ORDER BY val);
ALTER TABLE virt_rls_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_virt_rls_proj ON virt_rls_proj FOR SELECT USING _part_offset < 1 TO ALL;

SELECT '-- policy on a virtual column: read is refused';
SELECT id FROM mergeTreeProjection(currentDatabase(), 'virt_rls_proj', 'p') ORDER BY id; -- { serverError ACCESS_DENIED }

DROP ROW POLICY rp_virt_rls_proj ON virt_rls_proj;
DROP TABLE virt_rls_proj;
