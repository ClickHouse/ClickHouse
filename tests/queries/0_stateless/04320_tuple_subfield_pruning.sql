-- Tags: long, no-fasttest
-- Tags: no-random-settings, no-random-merge-tree-settings
-- ^ random settings can change part_type to Compact, but pruning only fires for Wide parts.

-- Tuple subfield pruning at INSERT and merge.
-- When all values of a named-Tuple subfield in a part are type-defaults, the writer
-- omits its stream files and narrows the part's `columns.txt` Tuple type. Reads use
-- CAST(narrowed_tuple, full_tuple) (already supported by the metadata-only ALTER work
-- in #107305) to materialize defaults.

DROP TABLE IF EXISTS t_tsp;

-- ============================================================
-- Case 1: flat named Tuple, one subfield all-default
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String, c Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, toString(number), 0) FROM numbers(100);

SELECT 'Case 1 (flat, one default subfield):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.a, t.b, t.c, t FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 2: nested named Tuple, inner subtuple all-default
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String, c2s Tuple(gold Int64, silver Int64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, toString(number), (0, 0)) FROM numbers(100);

SELECT 'Case 2 (nested, inner Tuple all-default):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.a, t.b, t.c2s.gold, t.c2s.silver, t.c2s FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 3: nested named Tuple, only one inner subfield all-default
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String, c2s Tuple(gold Int64, silver Int64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, toString(number), (number, 0)) FROM numbers(100);

SELECT 'Case 3 (nested, one inner subfield all-default):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.a, t.c2s.gold, t.c2s.silver FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 4: every named subfield non-default — must keep full schema
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, number + 1) FROM numbers(100);

SELECT 'Case 4 (all subfields non-default — keep full):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 5: whole top-level Tuple all-default — must KEEP column (no whole-column expiry)
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (0, '') FROM numbers(100);

SELECT 'Case 5 (whole top-level all-default — must keep column):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 6: Nullable subfield where every row is NULL
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Nullable(String), c Tuple(x Nullable(Int64), y Int64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, NULL, (NULL, 0)) FROM numbers(100);

SELECT 'Case 6 (Nullable subfield all-NULL):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.b, t.c.x, t.c.y, t FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 7: Array(Tuple) — every row is an empty array (subtree all-default)
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, arr Array(Tuple(x Int64, y String))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, []::Array(Tuple(x Int64, y String))) FROM numbers(100);

SELECT 'Case 7 (Array(Tuple) all empty):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.arr, length(t.arr) FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 8: Array(Tuple) with non-empty arrays — cannot prune sub-elements
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, arr Array(Tuple(x Int64, y Int64))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, [(0, 0), (0, 0)]) FROM numbers(100);

SELECT 'Case 8 (Array(Tuple) non-empty — keep full schema even if inner sub-elements are 0):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 9: Customer-like deep schema (mirrors 04319_named_tuple_metadata_only_alter Case 5)
-- ============================================================
CREATE TABLE t_tsp (
    id UInt64,
    event Tuple(
        common Tuple(uid Int64, ts DateTime),
        c2s Tuple(gold Int64, silver Int64, items Array(Tuple(slot Int64, amount Int64))),
        c2p Tuple(score Float32, level Int64)
    )
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- c2s.silver and c2p (whole) are zero; c2s.items is empty.
INSERT INTO t_tsp SELECT
    number,
    ((number, toDateTime('2024-01-01')), (number, 0, []::Array(Tuple(slot Int64, amount Int64))), (0, 0))
FROM numbers(100);

SELECT 'Case 9 (deep schema — multiple partial defaults):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 'event';
SELECT
    event.common.uid,
    event.c2s.gold,
    event.c2s.silver,
    event.c2s.items,
    event.c2p.score,
    event.c2p.level
FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 10: PARTITION BY where each partition populates a different subset
-- (the motivating use case)
-- ============================================================
CREATE TABLE t_tsp (
    kind String,
    id UInt64,
    data Tuple(common Int64, web Tuple(url String, clicks Int64), email Tuple(addr String, sends Int64))
)
ENGINE = MergeTree PARTITION BY kind ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp VALUES ('web', 1, (1, ('http://a', 100), ('', 0)));
INSERT INTO t_tsp VALUES ('email', 2, (2, ('', 0), ('a@b.c', 5)));

SELECT 'Case 10 (PARTITION BY narrows per partition):';
SELECT partition, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 'data'
ORDER BY partition;

SELECT 'Read full schema across partitions:';
SELECT kind, data FROM t_tsp ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 11: setting OFF — no pruning
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64, c Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_tuple_subfield_pruning = 0;

INSERT INTO t_tsp SELECT number, (number, 0, 0) FROM numbers(100);

SELECT 'Case 11 (setting OFF):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 12: Compact part — must KEEP full schema
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64, c Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000;

INSERT INTO t_tsp SELECT number, (number, 0, 0) FROM numbers(100);

SELECT 'Case 12 (Compact part — no pruning):';
SELECT name, part_type, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 13: Merge — both source parts pruned the same subfield: merged keeps it pruned
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String, c Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, toString(number), 0) FROM numbers(50);
INSERT INTO t_tsp SELECT number + 50, (number + 50, toString(number + 50), 0) FROM numbers(50);

SELECT 'Case 13 (merge — both parts pruned c, merged also pruned):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY type;

OPTIMIZE TABLE t_tsp FINAL;

SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY type;
SELECT t.a, t.b, t.c FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 14: Merge — one part pruned, the other did not: merged keeps full schema
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String, c Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, toString(number), 0) FROM numbers(50);          -- prunes c
INSERT INTO t_tsp SELECT number + 50, (number + 50, toString(number + 50), 7) FROM numbers(50);  -- keeps c

SELECT 'Case 14 (merge — one pruned, other not — merged keeps c):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY type;

OPTIMIZE TABLE t_tsp FINAL;

SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY type;
SELECT t.a, t.c FROM t_tsp WHERE id IN (1, 51) ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 15: Merge — different subfields pruned in different parts: merged takes the union
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, web Tuple(url String, clicks Int64), email Tuple(addr String, sends Int64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- part 1: prunes email
INSERT INTO t_tsp VALUES (1, (1, ('http://a', 100), ('', 0)));
-- part 2: prunes web
INSERT INTO t_tsp VALUES (2, (2, ('', 0), ('a@b.c', 5)));

SELECT 'Case 15 (merge — different parts pruned different subfields — merged takes union):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY type;

OPTIMIZE TABLE t_tsp FINAL;

SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY type;
SELECT t.a, t.web, t.email FROM t_tsp ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 16: Subcolumn read of a pruned subfield returns the default value
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String, c Int64, d Float64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, toString(number), 0, 0) FROM numbers(100);

SELECT 'Case 16 (subcolumn read of pruned subfield returns default):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.a, t.b, t.c, t.d FROM t_tsp WHERE id < 3 ORDER BY id;
-- Bulk aggregates over a pruned subfield should also be safe.
SELECT 'sum-of-zeros (defaults):', sum(t.c), sum(t.d) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 17: CHECK TABLE on a part with pruned subfields
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String, c2s Tuple(gold Int64, silver Int64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, toString(number), (0, 0)) FROM numbers(100);

SELECT 'Case 17 (CHECK TABLE accepts pruned-subfield part):';
CHECK TABLE t_tsp SETTINGS check_query_single_value_result = 0;

DROP TABLE t_tsp;

-- ============================================================
-- Case 18: Map(K, Tuple) — every row is an empty map
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, m Map(String, Tuple(x Int64, y String))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, map()::Map(String, Tuple(x Int64, y String))) FROM numbers(100);

SELECT 'Case 18 (Map(K, Tuple) all empty):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.m, length(mapKeys(t.m)) FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 19: Map(K, Tuple) — non-empty maps cannot prune sub-elements
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, m Map(String, Tuple(x Int64, y Int64))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, map('k', (number, 0))) FROM numbers(100);

SELECT 'Case 19 (Map(K, Tuple) non-empty — keep full schema):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 20: INSERT SELECT (instead of VALUES) still narrows
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64, c Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT n, tuple(n, 0, 0)::Tuple(a Int64, b Int64, c Int64)
FROM (SELECT number AS n FROM numbers(100));

SELECT 'Case 20 (INSERT SELECT narrows):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 21: Async INSERT path also narrows
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, 0) FROM numbers(50)
SETTINGS async_insert = 1, wait_for_async_insert = 1;

SELECT 'Case 21 (Async INSERT narrows):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 22: Materialized view writes through INSERT path — also narrows
-- ============================================================
CREATE TABLE t_tsp_src (id UInt64, v Int64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_tsp_dst (id UInt64, t Tuple(a Int64, b Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE MATERIALIZED VIEW t_tsp_mv TO t_tsp_dst AS
SELECT id, tuple(v, 0)::Tuple(a Int64, b Int64) AS t FROM t_tsp_src;

INSERT INTO t_tsp_src SELECT number, number FROM numbers(100);

SELECT 'Case 22 (Materialized view writes narrow):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp_dst' AND active AND column = 't';

DROP TABLE t_tsp_mv;
DROP TABLE t_tsp_dst;
DROP TABLE t_tsp_src;

-- ============================================================
-- Case 23: After metadata-only ALTER MODIFY adding a subfield, an INSERT whose
-- new subfield is all-default produces another narrowed part (so the new subfield
-- never gets physical storage). Merge of two narrowed parts stays narrowed.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, toString(number)) FROM numbers(50);

ALTER TABLE t_tsp MODIFY COLUMN t Tuple(a Int64, b String, c Int64);
SELECT 'mutations after ALTER:', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_tsp';

INSERT INTO t_tsp SELECT number + 50, (number + 50, toString(number + 50), 0) FROM numbers(50);

SELECT 'Case 23 (ALTER ADD subfield + INSERT default — both parts narrow):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY type;

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'After merge of two narrowed parts:';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.a, t.b, t.c FROM t_tsp WHERE id IN (0, 49, 50, 99) ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 24: ALTER UPDATE (mutation) on a narrowed part — by design, mutations
-- preserve the existing storage schema (re-materialize all subfields).
-- Result must be readable and correct.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, 0) FROM numbers(50);
SELECT 'Before UPDATE (narrowed):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

ALTER TABLE t_tsp UPDATE t = (t.a, 0) WHERE id < 50 SETTINGS mutations_sync = 2;

SELECT 'After UPDATE (mutate restores full schema, by design):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t FROM t_tsp WHERE id IN (0, 49) ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 25: Lightweight DELETE keeps the existing narrow type
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, 0) FROM numbers(50);

DELETE FROM t_tsp WHERE id = 5;

SELECT 'Case 25 (LWD preserves narrow type):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT count() FROM t_tsp;

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'After OPTIMIZE FINAL:';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT count(), max(t.a) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 26: ReplacingMergeTree merge — deduplication still narrows
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, 0) FROM numbers(50);
INSERT INTO t_tsp SELECT number, (number, 0) FROM numbers(50);

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'Case 26 (ReplacingMergeTree merge — still narrow):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT count() FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 27: Vertical merge algorithm — narrowing is preserved
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64, c Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         enable_vertical_merge_algorithm = 1,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_tsp SELECT number, (number, number, 0) FROM numbers(50);
INSERT INTO t_tsp SELECT number + 50, (number + 50, number + 50, 0) FROM numbers(50);

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'Case 27 (vertical merge keeps narrow):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t FROM t_tsp WHERE id IN (0, 99) ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 28: Two independent Tuple columns each narrow to their own subset
-- ============================================================
CREATE TABLE t_tsp (
    id UInt64,
    d1 Tuple(a Int64, b Int64),
    d2 Tuple(c Int64, d Int64),
    other Int64
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- d1.b = 0, d2.c = 0 — each column loses a different subfield
INSERT INTO t_tsp SELECT number, (number, 0), (0, number), number FROM numbers(50);

SELECT 'Case 28 (multiple Tuple columns prune independently):';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_tsp' AND active
ORDER BY column;

DROP TABLE t_tsp;

-- ============================================================
-- Case 29: Partial-default — even a single non-default row keeps the subfield
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- b is 0 for all rows except id=5 where b=42
INSERT INTO t_tsp SELECT number, (number, if(number = 5, 42, 0)) FROM numbers(100);

SELECT 'Case 29 (one non-default row preserves the subfield):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t FROM t_tsp WHERE id IN (4, 5, 6) ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 30: LowCardinality(String) subfield where every value is the empty string
-- (exercises `hasOnlyTypeDefaults` for `ColumnLowCardinality`)
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b LowCardinality(String), c LowCardinality(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, 'hello', '') FROM numbers(100);

SELECT 'Case 30 (LowCardinality(String) empty-string subfield is pruned):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT t.a, t.b, t.c FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 31: Column name containing dots vs. Tuple subfield paths — disambiguation
-- A top-level column named `data.deeper` must not be confused with the `data.deeper`
-- subfield path of a column named `data`.
-- ============================================================
CREATE TABLE t_tsp (
    `data` Tuple(a Int64, b Int64),
    `data.deeper` Tuple(x Int64, y Int64),
    id UInt64
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- data.b is all-default; data.deeper.y is all-default.
INSERT INTO t_tsp SELECT (number, 0)::Tuple(a Int64, b Int64), (number, 0)::Tuple(x Int64, y Int64), number FROM numbers(50);

SELECT 'Case 31 (top-level column with dot in name and a separate Tuple column):';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column IN ('data', 'data.deeper')
ORDER BY column;

DROP TABLE t_tsp;

-- ============================================================
-- Case 31b: Strong ownership-disambiguation regression. Two top-level columns
-- `data` (Tuple(a, deeper Tuple(y))) and `data.deeper` (Tuple(x, z)) both contribute
-- expired subfields. The leaf-`y` of `data.deeper` and the leaf-`y` of
-- `data.deeper` (the subfield inside `data`) would alias under a flat
-- longest-prefix scheme; ownership must be tracked per top-level column.
-- ============================================================
CREATE TABLE t_tsp (
    id UInt64,
    `data` Tuple(a Int64, deeper Tuple(y Int64)),
    `data.deeper` Tuple(x Int64, z Int64)
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Inside `data`: a has values, deeper.y is all-default → narrowed to Tuple(a Int64)
-- Inside `data.deeper`: x has values, z is all-default → narrowed to Tuple(x Int64)
INSERT INTO t_tsp SELECT number, (number, tuple(0))::Tuple(a Int64, deeper Tuple(y Int64)), (number, 0)::Tuple(x Int64, z Int64) FROM numbers(10);

SELECT 'Case 31b (overlapping dotted column names with their own subfields):';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column IN ('data', 'data.deeper')
ORDER BY column;
SELECT id, data, `data.deeper` FROM t_tsp ORDER BY id LIMIT 3;

DROP TABLE t_tsp;

-- ============================================================
-- Case 32: DETACH / ATTACH PARTITION preserves the narrowed per-part type
-- ============================================================
CREATE TABLE t_tsp (p UInt32, id UInt64, t Tuple(a Int64, b Int64))
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT 1, number, (number, 0) FROM numbers(50);
INSERT INTO t_tsp SELECT 2, number + 50, (number + 50, 0) FROM numbers(50);

SELECT 'Case 32 (DETACH/ATTACH preserves narrow):';
SELECT 'before:', partition, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY partition;

ALTER TABLE t_tsp DETACH PARTITION 1;
SELECT 'after detach:', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

ALTER TABLE t_tsp ATTACH PARTITION 1;
SELECT 'after attach:', partition, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't' ORDER BY partition;
SELECT count(), max(t.a) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 33: bytes_on_disk is smaller for the narrowed table
-- (sanity check that pruning actually saves space, not just metadata)
-- ============================================================
CREATE TABLE t_tsp_full (id UInt64, t Tuple(a Int64, b Int64, c Int64, d Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         enable_tuple_subfield_pruning = 0;

CREATE TABLE t_tsp_narrow (id UInt64, t Tuple(a Int64, b Int64, c Int64, d Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp_full   SELECT number, (number, 0, 0, 0) FROM numbers(10000);
INSERT INTO t_tsp_narrow SELECT number, (number, 0, 0, 0) FROM numbers(10000);

SELECT 'Case 33 (bytes_on_disk: narrowed < full):';
SELECT
    (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = currentDatabase() AND table = 't_tsp_narrow' AND active)
        <
    (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = currentDatabase() AND table = 't_tsp_full' AND active);

DROP TABLE t_tsp_full;
DROP TABLE t_tsp_narrow;

-- ============================================================
-- Case 34: INSERT producing two granules (multi-granule part) still narrows
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 1024;

INSERT INTO t_tsp SELECT number, (number, 0) FROM numbers(5000);

SELECT 'Case 34 (multi-granule part still narrows):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT count(), uniqExact(t.a), sum(t.b) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 35: Force-sparse serialization for every column + pruning. The reader
-- must preserve each kept subfield's `Sparse` kind in `serialization.json`;
-- otherwise reads return shifted data (the writer wrote sparse streams, while
-- a freshly-built default-kind info would dispatch the default deserializer).
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String, c Int64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 0.;

INSERT INTO t_tsp SELECT number, (number, toString(number), 0) FROM numbers(100);

SELECT 'Case 35 (force-sparse + pruning):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t.a, t.b, t.c, t FROM t_tsp WHERE id < 3 ORDER BY id;
SELECT min(id), max(id), min(t.a), max(t.a), sum(t.c) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 36: Force-sparse on a part where only an inner subfield is pruned (the kept
-- subfield's `Sparse` kind must survive the narrowing of the surrounding Tuple).
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, sub Tuple(x Int64, y Int64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 0.;

-- sub.y is all-default → sub is narrowed to Tuple(x Int64). sub.x must keep its sparse kind.
INSERT INTO t_tsp SELECT number, (number, (number, 0)) FROM numbers(100);

SELECT 'Case 36 (force-sparse + nested pruning):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t.sub.x, t.sub.y, t FROM t_tsp WHERE id < 3 ORDER BY id;
SELECT min(id), max(id), min(t.sub.x), max(t.sub.x), sum(t.sub.y) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 37: Regression — a Nullable subfield where every row is `toNullable(0)` /
-- `toNullable('')` is NOT pruned. `Nullable(X)`'s type-default is NULL, not
-- the wrapped 0 / ''; conflating the two would replay reads as NULL and lose data.
-- (`hasOnlyTypeDefaults` should report false because the null map is all zero.)
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(b Nullable(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, tuple(toNullable(0)) FROM numbers(100);

SELECT 'Case 37 (Nullable(UInt64) all toNullable(0) — keep, NOT pruned):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t, t.b, isNull(t.b) FROM t_tsp WHERE id < 3 ORDER BY id;
SELECT countIf(isNull(t.b)), countIf(t.b = 0) FROM t_tsp;

DROP TABLE t_tsp;

CREATE TABLE t_tsp (id UInt64, t Tuple(b Nullable(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, tuple(toNullable('')) FROM numbers(100);

SELECT 'Case 37 (Nullable(String) all toNullable(\'\') — keep, NOT pruned):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t, t.b, isNull(t.b) FROM t_tsp WHERE id < 3 ORDER BY id;
SELECT countIf(isNull(t.b)), countIf(t.b = '') FROM t_tsp;

DROP TABLE t_tsp;

CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b Nullable(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, NULL) FROM numbers(100);

SELECT 'Case 37 (b all NULL, a non-default — only b pruned):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t, t.b, isNull(t.b) FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 38: plain `String` subfield where every row is the empty string.
-- A regression that the `ColumnString::hasOnlyTypeDefaults` implementation
-- recognises empty strings (offsets are not cumulative-with-terminator in
-- ClickHouse `ColumnString`; an empty string contributes a zero offset, so all
-- offsets are zero when every string is empty).
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, '') FROM numbers(100);

SELECT 'Case 38 (plain String subfield all empty — pruned):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t.a, t.b, length(t.b) FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 39: Merge of two parts that both pruned a wrapper field (Array(Tuple))
-- must remove the wrapper `.size0` stream too — not just the inner leaf
-- streams. Both source parts have type `Tuple(a)` (the `arr` field was
-- pruned at INSERT time as all-empty). Merging must produce the same
-- narrowed `Tuple(a)` and the merged part must NOT contain a leftover
-- `t.arr.size0.bin`.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, arr Array(Tuple(x Int64, y String))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, []::Array(Tuple(x Int64, y String))) FROM numbers(50);
INSERT INTO t_tsp SELECT number + 50, (number + 50, []::Array(Tuple(x Int64, y String))) FROM numbers(50);

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'Case 39 (merge with wrapper-only fields pruned in all sources):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT count(), max(t.a), uniqExact(length(t.arr)) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 40: A `Tuple` element whose explicit name contains `.` would alias nested
-- subtree paths under the dotted-path scheme used by the pruning helpers
-- (e.g. element name `a.b` is indistinguishable from path `t.a.b` of element `b`
-- under element `a`). The collector must bail out for the whole `Tuple` so we
-- never silently drop the nested non-default values.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(`a.b` UInt64, a Tuple(b UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp VALUES (1, (0, (42))), (2, (0, (43))), (3, (0, (44)));

SELECT 'Case 40 (dotted-name Tuple element — type NOT narrowed):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 41: A `Tuple` subfield of type `Enum8` whose stored values are all the
-- enum's `0` representation must NOT be pruned indiscriminately — the read
-- path would materialize the missing subfield via `DataTypeEnum::insertDefaultInto`,
-- whose default is the smallest enum value (which may differ from `0` for shapes
-- like `Enum8('a' = 1)`). Conservatively refuse to prune any `Enum` subtree.
-- (Verified by Trace logging — without the guard the subfield `t.e` was pruned.)
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(id UInt64, e Enum8('def' = 1, 'zero' = 0)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, CAST('zero' AS Enum8('def' = 1, 'zero' = 0))) FROM numbers(100);

SELECT 'Case 41 (Enum subfield NOT pruned):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 42: A `Map(K, Tuple(...))` ALTER that adds a tuple subfield to the value
-- type must NOT cause the merge-side Sub-case A to prune the new leaf —
-- `SerializationMap` in `with_buckets` mode keeps its bucketed value streams
-- hidden from `enumerateStreams` (they're discovered only via column/state), so
-- prune cleanup cannot find or remove them. We instead treat the whole `Map`
-- as an opaque leaf at narrow time. Verified by ALTER ADD + merge: the merged
-- part keeps the full `Map(K, Tuple(...))` type, no stale `*.values.*` streams.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, m Map(String, Tuple(a Int64, b Int64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         map_serialization_version = 'with_buckets';

INSERT INTO t_tsp SELECT number, map('k', (number, 0)) FROM numbers(50);
ALTER TABLE t_tsp MODIFY COLUMN m Map(String, Tuple(a Int64, b Int64, c Int64));
INSERT INTO t_tsp SELECT number + 50, map('k', (number + 50, 0, 0)) FROM numbers(50);

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'Case 42 (Map(K, Tuple) ALTER + merge — full type preserved):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 'm';
SELECT count(), uniqExact(mapKeys(m)[1]) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 43: A `Map(K, Tuple(...))` wrapper nested inside a `Tuple` must NOT
-- be pruned even when the column is entirely empty. With
-- `map_serialization_version = 'with_buckets'`, the `Map`'s bucketed value
-- payload streams (`t.m.<bucket>.values.*`) are only discoverable from a
-- written column + state, so the type-only `enumerateStreams` used by the
-- prune cleanup path cannot find them. Pruning the wrapper would leave the
-- bucket streams on disk while `columns.txt` no longer references them.
-- Conservatively never prune a `Map` subtree.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a Int64, m Map(String, Tuple(x Int64, y Int64))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         map_serialization_version = 'with_buckets',
         max_buckets_in_map = 5, map_buckets_strategy = 'linear',
         map_buckets_min_avg_size = 1, map_buckets_coefficient = 1.0;

INSERT INTO t_tsp SELECT number, (number, map()::Map(String, Tuple(x Int64, y Int64))) FROM numbers(50);
INSERT INTO t_tsp SELECT number + 50, (number + 50, map()::Map(String, Tuple(x Int64, y Int64))) FROM numbers(50);

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'Case 43 (Map wrapper with_buckets — NOT pruned even when all empty):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT count(), max(t.a) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 44: `Enum` nested inside another `Tuple` whose stored bytes are all the
-- `0` enum representation must NOT be pruned via the outer `hasOnlyTypeDefaults`
-- shortcut — the read path would synthesize the wrong enum default. The
-- containing `Tuple` subtree is detected as "contains Enum" and the shortcut
-- recurses into element-by-element pruning instead.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(id UInt64, c Tuple(e Enum8('def' = 1, 'zero' = 0))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, (number, tuple(CAST('zero' AS Enum8('def' = 1, 'zero' = 0)))) FROM numbers(50);

SELECT 'Case 44 (nested Enum NOT pruned via outer shortcut):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT id, t FROM t_tsp WHERE id < 3 ORDER BY id;

DROP TABLE t_tsp;

-- ============================================================
-- Case 45: `Map` nested inside another `Tuple` that is otherwise entirely default
-- (with `map_serialization_version = 'with_buckets'`) must NOT be pruned via the
-- outer `hasOnlyTypeDefaults` shortcut — the bucketed payload streams would be
-- orphaned. The containing `Tuple` subtree is detected as "contains Map" and the
-- shortcut recurses into element-by-element pruning instead.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(id UInt64, c Tuple(m Map(String, Tuple(x Int64, y Int64)))))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         map_serialization_version = 'with_buckets',
         max_buckets_in_map = 5, map_buckets_strategy = 'linear',
         map_buckets_min_avg_size = 1, map_buckets_coefficient = 1.0;

INSERT INTO t_tsp SELECT number, (number, tuple(map()::Map(String, Tuple(x Int64, y Int64)))) FROM numbers(50);

SELECT 'Case 45 (nested Map with_buckets NOT pruned via outer shortcut):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';

DROP TABLE t_tsp;

-- ============================================================
-- Case 46: Merge-side Sub-case A must NOT prune a column whose storage type contains
-- a dotted-name `Tuple` element. Old part has narrower type `Tuple(a)`; after
-- metadata-only ALTER adds a `` `b.c` `` element, leaves would collapse to the parent
-- path and the missing `t.a` leaf in the old part would be promoted to a whole-column
-- removal — losing existing `t.a` values on `OPTIMIZE FINAL`.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_tsp SELECT number, tuple(number)::Tuple(a UInt64) FROM numbers(50);
ALTER TABLE t_tsp MODIFY COLUMN t Tuple(a UInt64, `b.c` UInt64);
INSERT INTO t_tsp SELECT number + 50, tuple(number + 50, 0)::Tuple(a UInt64, `b.c` UInt64) FROM numbers(50);

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'Case 46 (dotted Tuple element survives merge):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT count(), max(t.a) FROM t_tsp;

DROP TABLE t_tsp;

-- ============================================================
-- Case 47: Merge-side Sub-case A must NOT prune a column whose storage type contains
-- a `Map`. Old part lacks `m`; new part has it. Without the guard, merge would mark
-- the whole `m` subtree as expired in the merged part, and `removeEmptyColumnsFromPart`
-- would orphan bucketed map streams under `map_serialization_version = 'with_buckets'`.
-- ============================================================
CREATE TABLE t_tsp (id UInt64, t Tuple(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         map_serialization_version = 'with_buckets',
         max_buckets_in_map = 5, map_buckets_strategy = 'linear',
         map_buckets_min_avg_size = 1, map_buckets_coefficient = 1.0;

INSERT INTO t_tsp SELECT number, tuple(number)::Tuple(a UInt64) FROM numbers(50);
ALTER TABLE t_tsp MODIFY COLUMN t Tuple(a UInt64, m Map(String, Tuple(x Int64)));
INSERT INTO t_tsp SELECT number + 50, (number + 50, map())::Tuple(a UInt64, m Map(String, Tuple(x Int64))) FROM numbers(50);

OPTIMIZE TABLE t_tsp FINAL;

SELECT 'Case 47 (Map under named Tuple survives merge):';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tsp' AND active AND column = 't';
SELECT count(), max(t.a) FROM t_tsp;

DROP TABLE t_tsp;
