-- Tags: no-ordinary-database, no-async-insert, no-fasttest
-- UNIQUE KEY: which mutations run on the table and which are rejected.
--   1. ALTER DELETE, ALTER UPDATE of a key column or of `_row_exists`, a forced lightweight UPDATE, a mutation in a transaction: rejected
--   2. MATERIALIZE COLUMN: rejected for a key column; CLEAR COLUMN runs, a Nested group included unless it holds a key column (DROP too); CLEAR IF EXISTS of a missing one is a no-op
--   2a. part rewrites: APPLY DELETED MASK / PATCHES, MATERIALIZE PROJECTION are rejected
--   2b. CLEAR / MATERIALIZE / RENAME / MODIFY COLUMN, ALTER UPDATE: rejected for a column a MATERIALIZED key column reads, directly, through an ALIAS or as part of a Nested group; CLEAR under a DEFAULT key column runs
--   3. row-preserving ALTERs run: DROP / RENAME / MODIFY / MATERIALIZE COLUMN, DROP / MATERIALIZE INDEX and STATISTICS, REWRITE PARTS, UPDATE
--   4. plain table: the same operations still work without UNIQUE KEY
-- no-async-insert: after an async INSERT, a CLEAR of a Nested group can leave the arrays in place, as on
-- a plain MergeTree.

SET enable_unique_key = 1;

DROP TABLE IF EXISTS uk_mut_guard;
CREATE TABLE uk_mut_guard (a UInt32, b UInt32, c UInt32, d String DEFAULT 'def')
ENGINE = MergeTree ORDER BY (c) UNIQUE KEY (a, b);

INSERT INTO uk_mut_guard VALUES (1, 10, 100, 'x'), (2, 20, 200, 'y');

-- 1. ALTER DELETE / UPDATE: red if the mutation guard admits ALTER DELETE or an UPDATE of a key
-- column or of `_row_exists`.
SELECT 'alter_delete_uk' AS step;
ALTER TABLE uk_mut_guard DELETE WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'alter_update_uk_column' AS step;
ALTER TABLE uk_mut_guard UPDATE a = 99 WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'alter_update_row_exists' AS step;
ALTER TABLE uk_mut_guard UPDATE _row_exists = 0 WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

-- A lightweight update writes patch parts, which UNIQUE KEY does not support, so the forced mode fails.
SELECT 'lightweight_force_update_uk' AS step;
ALTER TABLE uk_mut_guard UPDATE d = 'z' WHERE a = 1
SETTINGS alter_update_mode = 'lightweight_force', enable_lightweight_update = 1; -- { serverError SUPPORT_IS_DISABLED }

-- Refused before the mutation is queued: the background task cannot run it, and the client would wait for good.
SELECT 'mutation_in_transaction_uk' AS step;
ALTER TABLE uk_mut_guard UPDATE d = 'z' WHERE 1 SETTINGS implicit_transaction = 1; -- { serverError SUPPORT_IS_DISABLED, NOT_IMPLEMENTED }
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'uk_mut_guard' AND NOT is_done;

-- 2. MATERIALIZE / CLEAR COLUMN: red if the mutation guard admits MATERIALIZE COLUMN of a key column
-- or rejects CLEAR COLUMN.
SELECT 'materialize_uk_a' AS step;
ALTER TABLE uk_mut_guard MATERIALIZE COLUMN a; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'materialize_uk_b' AS step;
ALTER TABLE uk_mut_guard MATERIALIZE COLUMN b; -- { serverError SUPPORT_IS_DISABLED }

-- A key column hits the key-column guard first (ALTER_OF_COLUMN_IS_FORBIDDEN).
SELECT 'clear_uk_a' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN a IN PARTITION ID 'all'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT 'clear_uk_b' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN b IN PARTITION ID 'all'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SET mutations_sync = 2;
SELECT 'clear_non_uk_d' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN d IN PARTITION ID 'all';
SELECT 'cleared_d', groupArray(d) FROM (SELECT d FROM uk_mut_guard ORDER BY a);

SELECT 'clear_missing_if_exists_noop' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN IF EXISTS nope IN PARTITION ID 'all';

-- 2a. part rewrites: red if the mutation guard admits one. It fires
-- before name resolution, so the names need not exist.
ALTER TABLE uk_mut_guard APPLY DELETED MASK; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_mut_guard APPLY PATCHES; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_mut_guard MATERIALIZE PROJECTION proj; -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM uk_mut_guard;  -- 2

DROP TABLE uk_mut_guard;

-- CLEAR of a Nested group names no physical column, only its `n.x` / `n.y` arrays.
-- Pinned Compact: a Wide part keeps the arrays, as on a plain MergeTree.
DROP TABLE IF EXISTS uk_mut_nested;
CREATE TABLE uk_mut_nested (id UInt32, n Nested(x UInt32, y String))
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '10G';

INSERT INTO uk_mut_nested VALUES (1, [1, 2], ['a', 'b']), (2, [3], ['c']);

SELECT 'clear_nested_group' AS step;
ALTER TABLE uk_mut_nested CLEAR COLUMN n IN PARTITION ID 'all';

SELECT id, n.x, n.y FROM uk_mut_nested ORDER BY id;

DROP TABLE uk_mut_nested;

-- A Nested group that holds a key column: CLEAR or DROP of the group would leave the key without its column.
DROP TABLE IF EXISTS uk_mut_nested_key;
CREATE TABLE uk_mut_nested_key (s UInt32, `n.id` UInt64)
ENGINE = MergeTree ORDER BY s UNIQUE KEY (`n.id`)
SETTINGS share_nested_offsets = 1;

SELECT 'clear_nested_group_with_key' AS step;
ALTER TABLE uk_mut_nested_key CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'drop_nested_group_with_key' AS step;
ALTER TABLE uk_mut_nested_key DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE uk_mut_nested_key;

-- Every prefix of a dotted key column names a group that holds it.
DROP TABLE IF EXISTS uk_mut_nested_prefix_key;
CREATE TABLE uk_mut_nested_prefix_key (s UInt32, `n.a.id` UInt64)
ENGINE = MergeTree ORDER BY s UNIQUE KEY (`n.a.id`)
SETTINGS share_nested_offsets = 1;

SELECT 'clear_inner_nested_group_with_key' AS step;
ALTER TABLE uk_mut_nested_prefix_key CLEAR COLUMN `n.a`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'drop_inner_nested_group_with_key' AS step;
ALTER TABLE uk_mut_nested_prefix_key DROP COLUMN `n.a`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE uk_mut_nested_prefix_key;

-- 2b. a column a key column is computed from: red if the guard admits a CLEAR, MATERIALIZE, UPDATE, RENAME or
-- MODIFY of it under a MATERIALIZED key column, or rejects a CLEAR under a DEFAULT key column, which the clear
-- does not recompute.
DROP TABLE IF EXISTS uk_mut_computed;
CREATE TABLE uk_mut_computed (id UInt32, a UInt64 DEFAULT id * 10, k UInt64 MATERIALIZED a * 2)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (k);
INSERT INTO uk_mut_computed (id, a) VALUES (1, 1), (2, 2);

SELECT 'clear_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT 'materialize_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed MATERIALIZE COLUMN a; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'update_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed UPDATE a = a + 10 WHERE 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'modify_type_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed MODIFY COLUMN a UInt32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- A queued CLEAR or UPDATE would recompute the key from the new expression.
SELECT 'modify_expression_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed MODIFY COLUMN a MATERIALIZED id * 30; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE uk_mut_computed MODIFY COLUMN a ALIAS id * 30; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The ALTER's mutations are checked against the old names: a CLEAR or UPDATE of the new name in the same ALTER would pass.
SELECT 'rename_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed RENAME COLUMN a TO a2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE uk_mut_computed;

-- The key reads `a` through the ALIAS `x`: retargeting `x` would make a queued UPDATE of `b` rewrite the key.
DROP TABLE IF EXISTS uk_mut_computed_alias;
CREATE TABLE uk_mut_computed_alias (id UInt32, a UInt64, b UInt64, x UInt64 ALIAS a, k UInt64 MATERIALIZED x)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (k);

SELECT 'modify_alias_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed_alias MODIFY COLUMN x ALIAS b; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'update_alias_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed_alias UPDATE a = a + 1 WHERE 1; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE uk_mut_computed_alias;

-- Without shared Nested offsets `n` and `n.a` are separate columns: reading `n.a` must not hide what `n` reads.
DROP TABLE IF EXISTS uk_mut_computed_prefix;
CREATE TABLE uk_mut_computed_prefix (id UInt32, b UInt64, n UInt64 MATERIALIZED b, `n.a` UInt64, k UInt64 MATERIALIZED n + `n.a`)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (k)
SETTINGS share_nested_offsets = 0;

SELECT 'update_source_behind_prefix_column' AS step;
ALTER TABLE uk_mut_computed_prefix UPDATE b = b + 1 WHERE 1; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE uk_mut_computed_prefix;

DROP TABLE IF EXISTS uk_mut_computed_nested;
CREATE TABLE uk_mut_computed_nested (id UInt32, n Nested(x UInt32, y UInt32), k UInt64 MATERIALIZED arraySum(n.x))
ENGINE = MergeTree ORDER BY id UNIQUE KEY (k)
SETTINGS share_nested_offsets = 1;

SELECT 'clear_nested_source_of_materialized_key' AS step;
ALTER TABLE uk_mut_computed_nested CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE uk_mut_computed_nested;

DROP TABLE IF EXISTS uk_mut_default_key;
CREATE TABLE uk_mut_default_key (id UInt32, a UInt64, k UInt64 DEFAULT a * 2)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (k);
INSERT INTO uk_mut_default_key (id, a) VALUES (1, 1), (2, 2);

SELECT 'clear_source_of_default_key' AS step;
ALTER TABLE uk_mut_default_key CLEAR COLUMN a;
SELECT 'default_key_kept', groupArray((a, k)) FROM (SELECT a, k FROM uk_mut_default_key ORDER BY id);
DROP TABLE uk_mut_default_key;

-- 3. row-preserving ALTERs: red if the mutation guard rejects one.
DROP TABLE IF EXISTS uk_mut_rewrite;
CREATE TABLE uk_mut_rewrite (id UInt64, v String, x UInt32, y UInt32, z UInt32, u UInt32, s UInt32 STATISTICS(minmax), INDEX ix x TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO uk_mut_rewrite SELECT number, 'a', number, number, number, number, number FROM numbers(1, 6);

SELECT 'drop_column' AS step;
ALTER TABLE uk_mut_rewrite DROP COLUMN y;
SELECT 'rename_column' AS step;
ALTER TABLE uk_mut_rewrite RENAME COLUMN z TO z2;
SELECT 'modify_column_type' AS step;
ALTER TABLE uk_mut_rewrite MODIFY COLUMN u UInt64;
SELECT 'materialize_index' AS step;
ALTER TABLE uk_mut_rewrite MATERIALIZE INDEX ix;
SELECT 'materialize_statistics' AS step;
ALTER TABLE uk_mut_rewrite MATERIALIZE STATISTICS s;
SELECT 'rewrite_parts' AS step;
ALTER TABLE uk_mut_rewrite REWRITE PARTS;
SELECT 'update_column' AS step;
ALTER TABLE uk_mut_rewrite UPDATE x = x + 1 WHERE 1;
SELECT 'drop_index' AS step;
ALTER TABLE uk_mut_rewrite DROP INDEX ix;
SELECT 'drop_statistics' AS step;
ALTER TABLE uk_mut_rewrite DROP STATISTICS s;

SELECT 'metadata_only_alters' AS step;
ALTER TABLE uk_mut_rewrite ADD COLUMN w String;
ALTER TABLE uk_mut_rewrite MODIFY COLUMN w String DEFAULT 'w';
ALTER TABLE uk_mut_rewrite COMMENT COLUMN v 'value';
SELECT 'materialize_column' AS step;
ALTER TABLE uk_mut_rewrite MATERIALIZE COLUMN w;

DROP TABLE uk_mut_rewrite;

-- 4. plain table: red if the mutation guard, on a mutation or on an ALTER, applies to a table
-- without UNIQUE KEY.
DROP TABLE IF EXISTS mt_plain;
CREATE TABLE mt_plain (a UInt32, b UInt32 DEFAULT 0, d String DEFAULT 'def')
ENGINE = MergeTree ORDER BY a;

INSERT INTO mt_plain VALUES (1, 10, 'x'), (2, 20, 'y');

SET mutations_sync = 2;
ALTER TABLE mt_plain MATERIALIZE COLUMN b;
ALTER TABLE mt_plain CLEAR COLUMN b IN PARTITION ID 'all';
ALTER TABLE mt_plain MATERIALIZE COLUMN d;
ALTER TABLE mt_plain CLEAR COLUMN d IN PARTITION ID 'all';

SELECT count() FROM mt_plain;  -- 2

DROP TABLE mt_plain;
