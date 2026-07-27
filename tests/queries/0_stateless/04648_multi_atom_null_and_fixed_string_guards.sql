-- Multi-atom key-condition extraction must keep the soundness guards that the single-atom
-- extraction had: the `isNull` atom is reused only for a bare key, and a `FixedString` constant
-- that compares zero-padded against a `String` (or narrower `FixedString`) key must not become a
-- point range. Both guards are checked on keys with several applicable key expressions, so every
-- candidate atom of one predicate leaf goes through them.

DROP TABLE IF EXISTS t_multi_atom_null;
DROP TABLE IF EXISTS t_multi_atom_fixed_string;
DROP TABLE IF EXISTS t_multi_atom_wide_fixed_string;

-- `d` is reachable bare and through `toYYYYMM` in the key, so one predicate leaf on `d` can emit
-- atoms for both key columns.
CREATE TABLE t_multi_atom_null (d Nullable(DateTime)) ENGINE = MergeTree ORDER BY (d, toYYYYMM(d)) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_multi_atom_null VALUES ('2026-07-01 00:00:00'), ('2026-07-02 00:00:00'), (NULL), ('2026-07-03 00:00:00');

-- The key column itself is compared, so the `isNull` atom applies and prunes to the NULL granule.
SELECT 'bare key <=> NULL';
SELECT count() FROM t_multi_atom_null WHERE d IS NOT DISTINCT FROM NULL;
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multi_atom_null WHERE d IS NOT DISTINCT FROM NULL) WHERE explain LIKE '%Granules: %/%';

-- Here the key is wrapped in a monotonic function that is not a key expression, so the `isNull`
-- atom (which ignores the chain) is not reused and the whole part is read.
SELECT 'wrapped key <=> NULL';
SELECT count() FROM t_multi_atom_null WHERE toDate(d) IS NOT DISTINCT FROM NULL;
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multi_atom_null WHERE toDate(d) IS NOT DISTINCT FROM NULL) WHERE explain LIKE '%Granules: %/%';

SELECT 'wrapped key IS NULL';
SELECT count() FROM t_multi_atom_null WHERE toDate(d) IS NULL;
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multi_atom_null WHERE toDate(d) IS NULL) WHERE explain LIKE '%Granules: %/%';

-- `s` is reachable both bare and through `lower` in the key.
CREATE TABLE t_multi_atom_fixed_string (s String) ENGINE = MergeTree ORDER BY (lower(s), s) SETTINGS index_granularity = 1;
INSERT INTO t_multi_atom_fixed_string VALUES ('abc'), ('abd'), ('xyz');

-- The `FixedString(8)` constant is the 8 bytes `abc\0\0\0\0\0` and compares zero-padded, so it
-- matches `abc`; a point range on the padded value would prune the matching granule.
SELECT 'FixedString constant vs String key';
SELECT count() FROM t_multi_atom_fixed_string WHERE s = toFixedString('abc', 8);
SELECT count() FROM t_multi_atom_fixed_string WHERE toFixedString('abc', 8) = s;
SELECT count() FROM t_multi_atom_fixed_string WHERE s IS NOT DISTINCT FROM toFixedString('abc', 8);
SELECT count() FROM t_multi_atom_fixed_string WHERE s != toFixedString('abc', 8);

-- A `FixedString` key at least as wide as the constant still pads it into exactly one key value,
-- so pruning stays exact there.
CREATE TABLE t_multi_atom_wide_fixed_string (s FixedString(8)) ENGINE = MergeTree ORDER BY (lower(s), s) SETTINGS index_granularity = 1;
INSERT INTO t_multi_atom_wide_fixed_string VALUES ('abc'), ('abd'), ('xyz');

SELECT 'FixedString constant vs wide FixedString key';
SELECT count() FROM t_multi_atom_wide_fixed_string WHERE s = toFixedString('abc', 8);
SELECT extract(explain, 'Granules: [0-9]+/[0-9]+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_multi_atom_wide_fixed_string WHERE s = toFixedString('abc', 8)) WHERE explain LIKE '%Granules: %/%';

DROP TABLE t_multi_atom_null;
DROP TABLE t_multi_atom_fixed_string;
DROP TABLE t_multi_atom_wide_fixed_string;
