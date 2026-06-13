-- Tags: long, no-fasttest
-- Tags: no-random-settings, no-random-merge-tree-settings
-- ^ random settings can change part_type to Compact, but we want to exercise the Wide path.

-- Metadata-only ALTER MODIFY COLUMN for named Tuple: appending subfields at any position
-- should not trigger a mutation and should not rewrite any existing parts.
--
-- Customer schema (from a real game-analytics workload): a deep tree of named tuples,
-- including `Array(Tuple(...))` nested inside another `Tuple(Tuple(...))`. The customer
-- routinely adds new event fields and previously waited minutes for each ALTER on multi-TB
-- tables.

DROP TABLE IF EXISTS t_named_tuple_alter;

-- ============================================================
-- Case 1: flat named Tuple, append subfield at the end
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter SELECT number, (number, toString(number)) FROM numbers(100);

SELECT 'Case 1 (flat, append at end):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, b String, c Nullable(Int64));
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.b, t.c FROM t_named_tuple_alter WHERE id < 3 ORDER BY id;
SELECT 't (whole):', t FROM t_named_tuple_alter WHERE id < 3 ORDER BY id;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 2: flat named Tuple, insert subfield in the middle
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter SELECT number, (number, toString(number)) FROM numbers(100);

SELECT 'Case 2 (flat, insert in middle):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, c Nullable(Int64), b String);
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.b, t.c FROM t_named_tuple_alter WHERE id < 3 ORDER BY id;
SELECT 't (whole):', t FROM t_named_tuple_alter WHERE id < 3 ORDER BY id;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 3: nested named Tuple, append at inner level
-- ============================================================
CREATE TABLE t_named_tuple_alter
(id UInt64, t Tuple(a Int64, sub Tuple(x Int64, y Int64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter SELECT number, (number, (number * 10, number * 100)) FROM numbers(100);

SELECT 'Case 3 (nested Tuple, inner append):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, sub Tuple(x Int64, y Int64, z Nullable(String)));
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.sub.x, t.sub.y, t.sub.z FROM t_named_tuple_alter WHERE id < 3 ORDER BY id;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 4: Array of named Tuple
-- ============================================================
CREATE TABLE t_named_tuple_alter
(id UInt64, t Array(Tuple(a Int64, b String)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter
SELECT number, arrayMap(x -> (x, toString(x)), range(3)) FROM numbers(100);

SELECT 'Case 4 (Array of Tuple):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Array(Tuple(a Int64, b String, c Nullable(Int64)));
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.b, t.c FROM t_named_tuple_alter WHERE id = 0;
SELECT 't (whole):', t FROM t_named_tuple_alter WHERE id = 0;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 5: Nested syntax with flatten_nested = 0
-- ============================================================
SET flatten_nested = 0;
CREATE TABLE t_named_tuple_alter
(id UInt64, items Nested(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter
SELECT number, arrayMap(x -> (x, toString(x)), range(3)) FROM numbers(100);

SELECT 'Case 5 (Nested with flatten_nested=0):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN items Nested(a Int64, b String, c Nullable(Int64));
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT items.a, items.b, items.c FROM t_named_tuple_alter WHERE id = 0;

DROP TABLE t_named_tuple_alter;
SET flatten_nested = 1;

-- ============================================================
-- Case 6: Map of named Tuple
-- ============================================================
CREATE TABLE t_named_tuple_alter
(id UInt64, t Map(String, Tuple(a Int64, b String)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, {'k1': (10, 'x'), 'k2': (20, 'y')});

SELECT 'Case 6 (Map of Tuple):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Map(String, Tuple(a Int64, b String, c Nullable(Int64)));
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t FROM t_named_tuple_alter;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 7: non-Nullable new subfield (default zero value, same as top-level ADD COLUMN)
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter SELECT number, (number, toString(number)) FROM numbers(100);

SELECT 'Case 7 (non-Nullable new subfield):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, b String, c Int64);
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.b, t.c FROM t_named_tuple_alter WHERE id < 3 ORDER BY id;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 8: multiple sequential ALTERs adding fields at varying positions
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter SELECT number, (number, toString(number)) FROM numbers(100);

ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, b String, c Nullable(Int64));
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, d Nullable(Float64), b String, c Nullable(Int64));
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, d Nullable(Float64), b String, c Nullable(Int64), e Array(Nullable(String)));

SELECT 'Case 8 (multiple sequential ALTERs):';
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.b, t.c, t.d, t.e FROM t_named_tuple_alter WHERE id < 2 ORDER BY id;
SELECT 't (whole):', t FROM t_named_tuple_alter WHERE id < 2 ORDER BY id;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 9 (REJECT): unnamed Tuple modification still requires mutation
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(Int64, String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 'x'));

SELECT 'Case 9 (unnamed Tuple rejected):';
-- Adding a third element to an unnamed Tuple requires a mutation (CAST fails today,
-- but that is the existing behavior we are preserving — not a regression).
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(Int64, String, Int64); -- { serverError TYPE_MISMATCH }

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 10 (REJECT): subfield removal triggers a mutation
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 'x'));

SELECT 'Case 10 (subfield removal triggers mutation):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64) SETTINGS alter_sync = 2;
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t FROM t_named_tuple_alter;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 11 (REJECT): subfield rename triggers a mutation (handled as drop + add today)
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 'x'));

SELECT 'Case 11 (subfield rename triggers mutation):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, c String) SETTINGS alter_sync = 2;
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 12 (REJECT): incompatible subfield type change triggers a mutation
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, tuple(42));

SELECT 'Case 12 (incompatible subfield type change triggers mutation):';
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a String) SETTINGS alter_sync = 2;
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t FROM t_named_tuple_alter;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 13: merge materializes the new subfield stream files for old parts
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter SELECT number, (number, toString(number)) FROM numbers(100);
ALTER TABLE t_named_tuple_alter MODIFY COLUMN t Tuple(a Int64, b String, c Nullable(Int64));

SELECT 'Case 13 before merge (parts count):';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_named_tuple_alter' AND active;

INSERT INTO t_named_tuple_alter VALUES (200, (200, 'two-hundred', 42));
OPTIMIZE TABLE t_named_tuple_alter FINAL;

SELECT 'Case 13 after merge: old rows return NULL, new row returns the inserted value:';
SELECT t.c FROM t_named_tuple_alter WHERE id IN (0, 1, 200) ORDER BY id;
SELECT 'Case 13 part count after merge:';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_named_tuple_alter' AND active;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 14 (REJECT): stream-name collision is still rejected
-- ============================================================
SET flatten_nested = 0;
CREATE TABLE t_named_tuple_alter (id UInt64, items Nested(a Int64, b String), `items.c` Int64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, [(1, 'a')], 99);

SELECT 'Case 14 (stream collision rejected):';
-- The new `c` subfield in `items` would generate stream "items.c" which collides
-- with the existing top-level `items.c` column. The pre-existing validation must reject.
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN items Nested(a Int64, b String, c Nullable(Int64)); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_named_tuple_alter;
SET flatten_nested = 1;

-- ============================================================
-- Case 15: customer schema — deeply nested Tuple/Array(Tuple)/Map combinations
-- with subfield additions at every depth. Each ALTER must complete instantly
-- with no mutation triggered.
-- ============================================================
CREATE TABLE t_named_tuple_alter
(
    `@timestamp` DateTime64(3),
    `_id` String,
    `data` Tuple(
        name Nullable(String),
        level Nullable(Int64),
        server_id Nullable(Int64),
        res_add Map(UInt32, Int64),
        c2s Tuple(
            int_value Nullable(Int64),
            server_id Nullable(Int64),
            statistics Tuple(
                kill_monster_num Nullable(Int64),
                gold Nullable(Int64),
                heros_statistics Array(Tuple(
                    hero_id Nullable(Int64),
                    star Nullable(Int64),
                    damage Nullable(Int64))))))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- 100 rows where every Nullable subfield is NULL, Map is empty, Array is empty.
-- The exact values are irrelevant — Case 15 only verifies that the four metadata-only
-- ALTERs below trigger zero mutations and that the freshly-added subfields read back
-- as defaults. (Earlier versions used `generateRandom()` here, but that table function
-- ignores the INSERT target schema in server mode and produces a random column count,
-- which makes the INSERT non-deterministic; see `NUMBER_OF_COLUMNS_DOESNT_MATCH`.)
INSERT INTO t_named_tuple_alter
SELECT
    toDateTime64('2024-01-01', 3),
    toString(number),
    (
        NULL, NULL, NULL,
        map(),
        (
            NULL, NULL,
            (
                NULL, NULL,
                []::Array(Tuple(hero_id Nullable(Int64), star Nullable(Int64), damage Nullable(Int64)))
            )
        )
    )
FROM numbers(100);

SELECT 'Case 15 customer schema before ALTERs:';
SELECT count() FROM t_named_tuple_alter;

-- Add a top-level subfield.
ALTER TABLE t_named_tuple_alter MODIFY COLUMN data Tuple(
    name Nullable(String), level Nullable(Int64), server_id Nullable(Int64),
    res_add Map(UInt32, Int64),
    c2s Tuple(int_value Nullable(Int64), server_id Nullable(Int64),
        statistics Tuple(kill_monster_num Nullable(Int64), gold Nullable(Int64),
            heros_statistics Array(Tuple(hero_id Nullable(Int64), star Nullable(Int64), damage Nullable(Int64))))),
    new_top Nullable(Int64));

-- Add a subfield inside the nested c2s Tuple.
ALTER TABLE t_named_tuple_alter MODIFY COLUMN data Tuple(
    name Nullable(String), level Nullable(Int64), server_id Nullable(Int64),
    res_add Map(UInt32, Int64),
    c2s Tuple(int_value Nullable(Int64), server_id Nullable(Int64),
        statistics Tuple(kill_monster_num Nullable(Int64), gold Nullable(Int64),
            heros_statistics Array(Tuple(hero_id Nullable(Int64), star Nullable(Int64), damage Nullable(Int64)))),
        new_c2s Nullable(Float64)),
    new_top Nullable(Int64));

-- Add a subfield inside the doubly-nested statistics Tuple.
ALTER TABLE t_named_tuple_alter MODIFY COLUMN data Tuple(
    name Nullable(String), level Nullable(Int64), server_id Nullable(Int64),
    res_add Map(UInt32, Int64),
    c2s Tuple(int_value Nullable(Int64), server_id Nullable(Int64),
        statistics Tuple(kill_monster_num Nullable(Int64), gold Nullable(Int64),
            heros_statistics Array(Tuple(hero_id Nullable(Int64), star Nullable(Int64), damage Nullable(Int64))),
            new_stat Nullable(String)),
        new_c2s Nullable(Float64)),
    new_top Nullable(Int64));

-- Add a subfield inside Array(Tuple(...)) — the deepest case.
ALTER TABLE t_named_tuple_alter MODIFY COLUMN data Tuple(
    name Nullable(String), level Nullable(Int64), server_id Nullable(Int64),
    res_add Map(UInt32, Int64),
    c2s Tuple(int_value Nullable(Int64), server_id Nullable(Int64),
        statistics Tuple(kill_monster_num Nullable(Int64), gold Nullable(Int64),
            heros_statistics Array(Tuple(hero_id Nullable(Int64), star Nullable(Int64), damage Nullable(Int64),
                new_hero Nullable(UInt8))),
            new_stat Nullable(String)),
        new_c2s Nullable(Float64)),
    new_top Nullable(Int64));

-- Add a subfield to a Map's value Tuple? Customer schema uses Map(UInt32, Int64) which
-- has a primitive value, so we cannot test Map<K, Tuple> additions on the customer
-- schema directly. Case 6 already covers that.

-- Verify every ALTER above is metadata-only.
SELECT 'Case 15 mutations after 4 ALTERs:';
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';

-- Verify all new subfields readable (and return defaults for the pre-ALTER part).
SELECT 'Case 15 new subfields readable:';
SELECT countIf(data.new_top IS NULL) AS top_null,
       countIf(data.c2s.new_c2s IS NULL) AS c2s_null,
       countIf(data.c2s.statistics.new_stat IS NULL) AS stat_null,
       countIf(arraySum(arrayMap(x -> if(x IS NULL, 1, 0), data.c2s.statistics.heros_statistics.new_hero)) > 0
               OR length(data.c2s.statistics.heros_statistics.new_hero) = 0) AS hero_default_count
FROM t_named_tuple_alter;

-- Verify whole-tuple read works (no LOGICAL_ERROR exception).
SELECT 'Case 15 whole tuple read works:';
SELECT count() FROM (SELECT data FROM t_named_tuple_alter);

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 16 (REJECT): key/index/projection safety checks must run even when the
-- type change would otherwise be metadata-only. If a subcolumn of the modified
-- column appears in the primary key or partition key, the ALTER must be rejected
-- to keep `primary.idx` self-consistent across parts.
-- ============================================================

-- Subcolumn in ORDER BY.
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, b UInt64))
ENGINE = MergeTree ORDER BY (t.a, id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 20));

SELECT 'Case 16a (subcolumn in ORDER BY rejected):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, b UInt64, c UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_named_tuple_alter;

-- Subcolumn in PARTITION BY.
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, b UInt64))
ENGINE = MergeTree PARTITION BY t.a ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 20));

SELECT 'Case 16b (subcolumn in PARTITION BY rejected):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, b UInt64, c UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_named_tuple_alter;

-- Whole Tuple column in ORDER BY (`primary.idx` would have the wrong arity).
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, b UInt64))
ENGINE = MergeTree ORDER BY (t, id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 20));

SELECT 'Case 16c (whole Tuple in ORDER BY rejected):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, b UInt64, c UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_named_tuple_alter;

-- Sanity: appending a subfield to a Tuple that is NOT in any key still works.
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, b UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 20));

SELECT 'Case 16d (Tuple not in any key still works):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, b UInt64, c Nullable(UInt64));
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.b, t.c FROM t_named_tuple_alter;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 17 (REJECT): nested-Tuple subfield additions still trigger the key/index
-- safety check. The outer Tuple has the same field count, but a nested Tuple
-- gains a field, which is still a metadata-only conversion that changes the
-- on-disk shape of any embedded Tuple value in a key column.
-- ============================================================

-- A subcolumn that is *underneath* the nested addition is part of ORDER BY.
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, sub Tuple(x UInt64)))
ENGINE = MergeTree ORDER BY (t.sub.x, id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, (100)));

SELECT 'Case 17a (nested addition, subcolumn under it in ORDER BY rejected):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, sub Tuple(x UInt64, y UInt64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_named_tuple_alter;

-- The whole Tuple column is in ORDER BY and a nested Tuple inside gains a field.
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, sub Tuple(x UInt64)))
ENGINE = MergeTree ORDER BY (t, id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, (100)));

SELECT 'Case 17b (nested addition with whole Tuple in ORDER BY rejected):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, sub Tuple(x UInt64, y UInt64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_named_tuple_alter;

-- A sibling subcolumn (not under the nested addition) is in ORDER BY. The check
-- still must reject because the modified column as a whole appears in keys.
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, sub Tuple(x UInt64)))
ENGINE = MergeTree ORDER BY (t.a, id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, (100)));

SELECT 'Case 17c (nested addition, sibling subcolumn in ORDER BY rejected):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, sub Tuple(x UInt64, y UInt64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_named_tuple_alter;

-- Nested addition with no key dependency — must succeed and be metadata-only.
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, sub Tuple(x UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, (100)));

SELECT 'Case 17d (nested addition, no key dep, succeeds as metadata-only):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, sub Tuple(x UInt64, y Nullable(UInt64)));
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.sub.x, t.sub.y FROM t_named_tuple_alter;

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 18 (REJECT): explicit skip indexes on the whole column block Tuple
-- subfield additions, because the on-disk index bytes (`skp_idx_*.idx`) are
-- serialized with the old tuple shape and metadata-only ALTER never refreshes
-- them via `MutationsInterpreter`. Rejected unconditionally, independent of
-- the `alter_column_secondary_index_mode` setting.
-- ============================================================

CREATE TABLE t_named_tuple_alter
(id UInt64, t Tuple(a UInt64, b UInt64), INDEX idx_t t TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 20));

SELECT 'Case 18a (explicit set skip index on whole column rejected, default mode):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, b UInt64, c UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Even with `rebuild` mode, reject — the rebuild path is mutation-based and the
-- metadata-only Tuple addition does not emit any `MutationCommand` for it.
SELECT 'Case 18b (explicit skip index rejected even with rebuild mode):';
ALTER TABLE t_named_tuple_alter MODIFY SETTING alter_column_secondary_index_mode = 'rebuild';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(a UInt64, b UInt64, c UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_named_tuple_alter;

-- ============================================================
-- Case 19: pure reorder of existing fields (without any additions) is not
-- metadata-only. `primary.idx` bytes and explicit skip-index bytes for any
-- embedded tuple value are serialized in the old field order; the new type
-- deserializes and compares in the new order. Even on a column not in any
-- key, the relative order of existing fields must be preserved to take the
-- metadata-only path.
-- ============================================================
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, b UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 20));

SELECT 'Case 19a (pure reorder triggers mutation):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(b UInt64, a UInt64) SETTINGS alter_sync = 2;
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t FROM t_named_tuple_alter;

DROP TABLE t_named_tuple_alter;

-- Insertion preserving relative order is still metadata-only.
CREATE TABLE t_named_tuple_alter (id UInt64, t Tuple(a UInt64, b UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_named_tuple_alter VALUES (1, (10, 20));

SELECT 'Case 19b (insertion preserving order is metadata-only):';
ALTER TABLE t_named_tuple_alter
    MODIFY COLUMN t Tuple(c Nullable(UInt64), a UInt64, b UInt64);
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_alter';
SELECT t.a, t.b, t.c FROM t_named_tuple_alter;

DROP TABLE t_named_tuple_alter;
