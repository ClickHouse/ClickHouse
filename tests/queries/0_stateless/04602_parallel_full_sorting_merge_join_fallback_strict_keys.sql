-- Regression coverage for the `join_algorithm` priority-list fallback contract of
-- `parallel_full_sorting_merge`. Strict join-key inference (`TableJoin` `require_strict_keys_match`)
-- is engaged by *list membership*, before the actual algorithm is selected, so
-- `join_algorithm = 'hash,parallel_full_sorting_merge'` takes the strict-key path even when the
-- selected join ends up being plain `hash`. That gate deliberately mirrors the pre-existing
-- `full_sorting_merge` list-membership gate (on master: `require_strict_keys_match =
-- isEnabledAlgorithm(FULL_SORTING_MERGE)`), so the invariant pinned here is: adding
-- `parallel_full_sorting_merge` as a lower-priority fallback behaves EXACTLY like the analogous
-- pre-existing `hash,full_sorting_merge` configuration - no new divergence relative to master.
--
-- Observable strict-key effects exist only under the legacy analyzer:
--   1. `USING` promotes keys that differ by nullability / `LowCardinality` to the common supertype
--      (plain `hash` keeps the left table's type).
--   2. Special-storage joins (`Join` engine right table with the narrower key type) throw
--      `Can't change type for right table` because the right side cannot be converted.
-- The analyzer resolves `USING` supertypes independently of the strict-key gate, so all three
-- configurations must behave identically there.

SET join_use_nulls = 0;

DROP TABLE IF EXISTS pfsmj_fb_left_str;
DROP TABLE IF EXISTS pfsmj_fb_right_nullable;
DROP TABLE IF EXISTS pfsmj_fb_left_lc;
DROP TABLE IF EXISTS pfsmj_fb_right_str;
DROP TABLE IF EXISTS pfsmj_fb_left_nullable;
DROP TABLE IF EXISTS pfsmj_fb_join;

CREATE TABLE pfsmj_fb_left_str (k String, lv UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE pfsmj_fb_right_nullable (k Nullable(String), rv UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pfsmj_fb_left_str VALUES ('a', 1), ('b', 2);
INSERT INTO pfsmj_fb_right_nullable VALUES ('a', 10), (NULL, 20);

CREATE TABLE pfsmj_fb_left_lc (k LowCardinality(String), lv UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE pfsmj_fb_right_str (k String, rv UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pfsmj_fb_left_lc VALUES ('a', 1), ('b', 2);
INSERT INTO pfsmj_fb_right_str VALUES ('a', 10), ('c', 20);

CREATE TABLE pfsmj_fb_left_nullable (k Nullable(String), lv UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE pfsmj_fb_join (k String, rv UInt8) ENGINE = Join(ALL, INNER, k);
INSERT INTO pfsmj_fb_left_nullable VALUES ('a', 1), (NULL, 2);
INSERT INTO pfsmj_fb_join VALUES ('a', 10), ('b', 20);

-- Legacy analyzer: the strict-key effects are observable, and the fallback configuration with the
-- new algorithm must match the pre-existing one exactly.
SET enable_analyzer = 0;

-- `USING` nullability: plain `hash` keeps `String`; both merge-join fallbacks promote to `Nullable(String)`.
SELECT 'legacy using hash', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_str INNER JOIN pfsmj_fb_right_nullable USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT 'legacy using hash_fsm', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_str INNER JOIN pfsmj_fb_right_nullable USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,full_sorting_merge';
SELECT 'legacy using hash_pfsm', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_str INNER JOIN pfsmj_fb_right_nullable USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,parallel_full_sorting_merge';

-- `USING` `LowCardinality`: plain `hash` keeps `LowCardinality(String)`; both fallbacks promote to `String`.
SELECT 'legacy lc hash', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_lc INNER JOIN pfsmj_fb_right_str USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT 'legacy lc hash_fsm', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_lc INNER JOIN pfsmj_fb_right_str USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,full_sorting_merge';
SELECT 'legacy lc hash_pfsm', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_lc INNER JOIN pfsmj_fb_right_str USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,parallel_full_sorting_merge';

-- Special storage: the right key `String` would have to widen to `Nullable(String)`, which is not
-- allowed for a `Join` engine table. Plain `hash` succeeds (no strict inference); both merge-join
-- fallbacks throw the same `TYPE_MISMATCH` - identically for the pre-existing and the new algorithm.
SELECT 'legacy storage_join hash', k, lv, rv FROM pfsmj_fb_left_nullable INNER JOIN pfsmj_fb_join USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT k FROM pfsmj_fb_left_nullable INNER JOIN pfsmj_fb_join USING (k) SETTINGS join_algorithm = 'hash,full_sorting_merge'; -- { serverError TYPE_MISMATCH }
SELECT k FROM pfsmj_fb_left_nullable INNER JOIN pfsmj_fb_join USING (k) SETTINGS join_algorithm = 'hash,parallel_full_sorting_merge'; -- { serverError TYPE_MISMATCH }

-- Analyzer: `USING` supertype resolution does not depend on the strict-key gate, so all three
-- configurations behave identically.
SET enable_analyzer = 1;

SELECT 'analyzer using hash', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_str INNER JOIN pfsmj_fb_right_nullable USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT 'analyzer using hash_fsm', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_str INNER JOIN pfsmj_fb_right_nullable USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,full_sorting_merge';
SELECT 'analyzer using hash_pfsm', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_str INNER JOIN pfsmj_fb_right_nullable USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,parallel_full_sorting_merge';

SELECT 'analyzer lc hash', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_lc INNER JOIN pfsmj_fb_right_str USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT 'analyzer lc hash_fsm', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_lc INNER JOIN pfsmj_fb_right_str USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,full_sorting_merge';
SELECT 'analyzer lc hash_pfsm', toTypeName(k), k, lv, rv FROM pfsmj_fb_left_lc INNER JOIN pfsmj_fb_right_str USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,parallel_full_sorting_merge';

SELECT 'analyzer storage_join hash', k, lv, rv FROM pfsmj_fb_left_nullable INNER JOIN pfsmj_fb_join USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT 'analyzer storage_join hash_fsm', k, lv, rv FROM pfsmj_fb_left_nullable INNER JOIN pfsmj_fb_join USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,full_sorting_merge';
SELECT 'analyzer storage_join hash_pfsm', k, lv, rv FROM pfsmj_fb_left_nullable INNER JOIN pfsmj_fb_join USING (k) ORDER BY k SETTINGS join_algorithm = 'hash,parallel_full_sorting_merge';

DROP TABLE pfsmj_fb_left_str;
DROP TABLE pfsmj_fb_right_nullable;
DROP TABLE pfsmj_fb_left_lc;
DROP TABLE pfsmj_fb_right_str;
DROP TABLE pfsmj_fb_left_nullable;
DROP TABLE pfsmj_fb_join;
