-- Tags: no-parallel

-- Pins the user-visible `Overlay` lookup-precedence contract:
--   * when several sources contain a table with the same name, the source listed earlier wins;
--   * duplicate source names are collapsed, keeping the first occurrence.
-- Regression guard: an earlier version of this branch resolved the sources in alphabetical order
-- instead of the order the user wrote them, and the sibling suite
-- `03611_overlay_database_server_side.sql` never puts the same table name in two sources, so it
-- would have stayed green. This test creates the same table `t` in two sources to prove the rule.

DROP DATABASE IF EXISTS db_overlay_precedence;
DROP DATABASE IF EXISTS db_src_first;
DROP DATABASE IF EXISTS db_src_second;

CREATE DATABASE db_src_first ENGINE = Atomic;
CREATE DATABASE db_src_second ENGINE = Atomic;

-- Both sources define a table named `t`; the `origin` column records which source a row came from.
CREATE TABLE db_src_first.t (id UInt32, origin String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE db_src_second.t (id UInt32, origin String) ENGINE = MergeTree ORDER BY id;
INSERT INTO db_src_first.t VALUES (1, 'first');
INSERT INTO db_src_second.t VALUES (2, 'second');

-- `db_src_first` is listed first, so `db_overlay_precedence.t` resolves to `db_src_first.t`
-- (the `t` in `db_src_second` is shadowed, not unioned).
CREATE DATABASE db_overlay_precedence ENGINE = Overlay('db_src_first', 'db_src_second');
SELECT 'listed-first-wins', id, origin FROM db_overlay_precedence.t ORDER BY id;
DROP DATABASE db_overlay_precedence;

-- Reversing the source order flips which source wins for the same table name.
CREATE DATABASE db_overlay_precedence ENGINE = Overlay('db_src_second', 'db_src_first');
SELECT 'reversed-order-wins', id, origin FROM db_overlay_precedence.t ORDER BY id;
DROP DATABASE db_overlay_precedence;

-- Duplicate source names are collapsed, keeping the first occurrence: `SHOW CREATE DATABASE` renders
-- the deduplicated source list, and precedence still follows the first occurrence, so `t` comes from
-- `db_src_first`.
CREATE DATABASE db_overlay_precedence ENGINE = Overlay('db_src_first', 'db_src_second', 'db_src_first');
SHOW CREATE DATABASE db_overlay_precedence;
SELECT 'dedup-first-occurrence-wins', id, origin FROM db_overlay_precedence.t ORDER BY id;
DROP DATABASE db_overlay_precedence;

DROP DATABASE db_src_first;
DROP DATABASE db_src_second;
