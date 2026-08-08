-- { echo }
-- Tags: memory-engine
-- Tag memory-engine: with echo the statement text is the output, so rewriting ENGINE = Memory to
-- MergeTree would diverge from the reference
SET allow_suspicious_low_cardinality_types = 1;
SET join_use_nulls = 0; -- CI may inject True; the Join engine rejects a mismatched join_use_nulls (StorageJoin.cpp getJoinLocked), and a LEFT-join miss would read NULL instead of 0
SET enable_analyzer = 1; -- the fix lives in the analyzer's plan path; the old analyzer refuses this shape with TYPE_MISMATCH

-- Drop first: the stress job runs some workers with one shared database for every test
-- (stress.py --database=test_N), where clickhouse-test neither creates nor drops a per-test
-- database, so a second run would hit TABLE_ALREADY_EXISTS.
DROP TABLE IF EXISTS jt;
DROP TABLE IF EXISTS big;
DROP TABLE IF EXISTS s_u32;
DROP TABLE IF EXISTS o_u64;
DROP TABLE IF EXISTS l_u64;
DROP TABLE IF EXISTS l_i64;
DROP TABLE IF EXISTS s_nu32;
DROP TABLE IF EXISTS l_u64_oor;
DROP TABLE IF EXISTS s_mk;
DROP TABLE IF EXISTS l_mk;
DROP TABLE IF EXISTS o_mk;
DROP TABLE IF EXISTS s_u64;
DROP TABLE IF EXISTS l_u32;
DROP TABLE IF EXISTS l_nu32;
DROP TABLE IF EXISTS s_rj;
DROP TABLE IF EXISTS l_rj;
DROP TABLE IF EXISTS s_str;
DROP TABLE IF EXISTS l_fs;
DROP TABLE IF EXISTS l_f64;
DROP TABLE IF EXISTS s_i32;
DROP TABLE IF EXISTS l_dec;
DROP TABLE IF EXISTS s_dt;
DROP TABLE IF EXISTS l_dt64;
DROP TABLE IF EXISTS s_lc;
DROP TABLE IF EXISTS l_u32_lc;
DROP TABLE IF EXISTS s_i16;
DROP TABLE IF EXISTS l_u32_ms;
DROP TABLE IF EXISTS l_u128;
DROP TABLE IF EXISTS l_nu64;
DROP TABLE IF EXISTS s_u32b;
DROP TABLE IF EXISTS l_lcnu64;
DROP TABLE IF EXISTS o_nu32;
DROP TABLE IF EXISTS l_lcu32;
DROP TABLE IF EXISTS l_lcu64;
DROP TABLE IF EXISTS s_r32;
DROP TABLE IF EXISTS o_r32;
DROP TABLE IF EXISTS l_r64;
DROP TABLE IF EXISTS s_f32;
DROP TABLE IF EXISTS o_f32;
DROP TABLE IF EXISTS s_semi;
DROP TABLE IF EXISTS o_semi;
DROP TABLE IF EXISTS s_bool;
DROP TABLE IF EXISTS l_i64_b;
DROP TABLE IF EXISTS s_nbool;
DROP TABLE IF EXISTS l_i64_b3;
DROP TABLE IF EXISTS s_u8;
DROP TABLE IF EXISTS l_bool;

-- 1. The reported query (issue #104918) returns the correct answer instead of INCOMPATIBLE_TYPE_OF_JOIN.
CREATE TABLE jt (k UInt32, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO jt SELECT number * 2, number * 5 - 100 FROM numbers(500);
CREATE TABLE big (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO big SELECT number, number FROM numbers(1000);
SELECT count(), count(r.jv), sum(r.jv) FROM big AS l ANY LEFT JOIN jt AS r USING (k);

-- 2. Correctness against an ENGINE = Memory oracle, with left keys outside the UInt32 domain.
-- A plain CAST would wrap those keys and fabricate matches (100 / [30,10,20,0,30,10]).
CREATE TABLE s_u32 (k UInt32, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_u32 VALUES (0, 30), (2, 10), (4, 20);
CREATE TABLE o_u64 (k UInt64, jv Int64) ENGINE = Memory;
INSERT INTO o_u64 VALUES (0, 30), (2, 10), (4, 20);
CREATE TABLE l_u64 (k UInt64) ENGINE = Memory;
INSERT INTO l_u64 VALUES (0), (2), (4), (7), (4294967298), (4294967296);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64 AS l ANY LEFT JOIN o_u64 AS r USING (k);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64 AS l ANY LEFT JOIN s_u32 AS r USING (k);

-- 3. The direct StorageJoin lookup is kept, i.e. the fix does not degrade to a generic hash join.
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_u64 AS l ANY LEFT JOIN s_u32 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
-- and the left key really is the side that carries the conversion
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1
    SELECT sum(r.jv) FROM l_u64 AS l ANY LEFT JOIN s_u32 AS r USING (k))
WHERE explain ILIKE '%accurateCastOrNull%';

-- 4. Same join in the ON spelling.
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64 AS l ANY LEFT JOIN s_u32 AS r ON l.k = r.k;

-- 5. Negative left values match nothing.
CREATE TABLE l_i64 (k Int64) ENGINE = Memory;
INSERT INTO l_i64 VALUES (-5), (2);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_i64 AS l ANY LEFT JOIN s_u32 AS r USING (k);

-- 6. Nullable storage key: an out-of-range left key must not match the NULL storage key.
CREATE TABLE s_nu32 (k Nullable(UInt32), jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_nu32 VALUES (2, 10), (4, 20), (NULL, 99);
CREATE TABLE l_u64_oor (k UInt64) ENGINE = Memory;
INSERT INTO l_u64_oor VALUES (2), (4), (4294967298);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64_oor AS l ANY LEFT JOIN s_nu32 AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_u64_oor AS l ANY LEFT JOIN s_nu32 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';

-- 7. Multi-key storage with one narrowing key. The generic hash join cannot read such a table
-- as a source (UNSUPPORTED_JOIN_KEYS), so keeping the direct path is the only way to answer this.
CREATE TABLE s_mk (a UInt32, b String, jv Int64) ENGINE = Join(ANY, LEFT, a, b);
INSERT INTO s_mk VALUES (2, 'x', 10);
CREATE TABLE l_mk (a UInt64, b String) ENGINE = Memory;
INSERT INTO l_mk VALUES (2, 'x'), (4294967298, 'x');
CREATE TABLE o_mk (a UInt64, b String, jv Int64) ENGINE = Memory;
INSERT INTO o_mk VALUES (2, 'x', 10);
SELECT sum(r.jv), arraySort(groupArray((l.a, r.jv))) FROM l_mk AS l ANY LEFT JOIN o_mk AS r ON l.a = r.a AND l.b = r.b;
SELECT sum(r.jv), arraySort(groupArray((l.a, r.jv))) FROM l_mk AS l ANY LEFT JOIN s_mk AS r ON l.a = r.a AND l.b = r.b;
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_mk AS l ANY LEFT JOIN s_mk AS r ON l.a = r.a AND l.b = r.b)
WHERE explain ILIKE '%FilledJoin%';

-- 8. Non-regression pins: queries that already work must keep working unchanged.
CREATE TABLE s_u64 (k UInt64, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_u64 VALUES (2, 10), (4, 20);
-- widening direction (storage key wider than the left key)
CREATE TABLE l_u32 (k UInt32) ENGINE = Memory;
INSERT INTO l_u32 VALUES (2), (4), (7);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u32 AS l ANY LEFT JOIN s_u64 AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_u32 AS l ANY LEFT JOIN s_u64 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
-- type-matched direct join
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64_oor AS l ANY LEFT JOIN s_u64 AS r USING (k);
-- Same width, nullability only. This must NOT take the narrowing path: no conversion is needed on
-- either side, and rewriting the left key here would change the plan of every such existing join
-- (it regressed 03786_storage_join_type_conversion).
CREATE TABLE l_nu32 (k Nullable(UInt32)) ENGINE = Memory;
INSERT INTO l_nu32 VALUES (2), (NULL);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_nu32 AS l ANY LEFT JOIN s_u32 AS r USING (k);
SELECT count() FROM (EXPLAIN PLAN actions = 1
    SELECT sum(r.jv) FROM l_nu32 AS l ANY LEFT JOIN s_u32 AS r USING (k))
WHERE explain ILIKE '%accurateCastOrNull%';
-- the mirrored shape a RIGHT join produces (a Nullable left key against a non-Nullable storage key)
CREATE TABLE s_rj (x UInt32, s String) ENGINE = Join(ALL, RIGHT, x);
INSERT INTO s_rj VALUES (1, 'a');
CREATE TABLE l_rj (x Nullable(UInt32), str String) ENGINE = Memory;
INSERT INTO l_rj VALUES (1, 'l');
SELECT count() FROM (EXPLAIN PLAN actions = 1
    SELECT * FROM l_rj ALL RIGHT JOIN s_rj USING (x))
WHERE explain ILIKE '%accurateCastOrNull%';
-- FixedString left key against a String storage key
CREATE TABLE s_str (k String, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_str VALUES ('abcd', 1);
CREATE TABLE l_fs (k FixedString(4)) ENGINE = Memory;
INSERT INTO l_fs VALUES ('abcd');
SELECT sum(r.jv) FROM l_fs AS l ANY LEFT JOIN s_str AS r USING (k);

-- 9. Declining carriers keep raising INCOMPATIBLE_TYPE_OF_JOIN. accurateCastOrNull is not a pure
-- domain check for these, so admitting them would silently fabricate matches.
CREATE TABLE l_f64 (k Float64) ENGINE = Memory;
INSERT INTO l_f64 VALUES (2), (1.5);
SELECT sum(r.jv) FROM l_f64 AS l ANY LEFT JOIN s_u32 AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
CREATE TABLE s_i32 (k Int32, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_i32 VALUES (1, 100), (2, 200);
CREATE TABLE l_dec (k Decimal64(1)) ENGINE = Memory;
INSERT INTO l_dec VALUES (1.0), (1.5), (2.0);
SELECT sum(r.jv) FROM l_dec AS l ANY LEFT JOIN s_i32 AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
CREATE TABLE s_dt (k DateTime, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_dt VALUES ('2020-01-01 00:00:00', 7);
CREATE TABLE l_dt64 (k DateTime64(3)) ENGINE = Memory;
INSERT INTO l_dt64 VALUES ('2020-01-01 00:00:00.500');
SELECT sum(r.jv) FROM l_dt64 AS l ANY LEFT JOIN s_dt AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- LowCardinality storage keys are refused for any non-LowCardinality left key. That is a separate
-- pre-existing defect: the control below shows a type-matched left key is refused too.
CREATE TABLE s_lc (k LowCardinality(UInt32), jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_lc VALUES (2, 10), (4, 20);
SELECT sum(r.jv) FROM l_u64_oor AS l ANY LEFT JOIN s_lc AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
CREATE TABLE l_u32_lc (k UInt32) ENGINE = Memory;
INSERT INTO l_u32_lc VALUES (2);
SELECT sum(r.jv) FROM l_u32_lc AS l ANY LEFT JOIN s_lc AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- Mixed signedness: getLeastSupertype promotes a signed/unsigned pair to a type wider than both
-- operands, so the common type is not the left key type and the narrowing rewrite declines. Today's
-- refusal stands for these pairs; that is a recorded residual, not a silent gap.
CREATE TABLE s_i16 (k Int16, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_i16 VALUES (2, 10);
CREATE TABLE l_u32_ms (k UInt32) ENGINE = Memory;
INSERT INTO l_u32_ms VALUES (2), (100000);
SELECT sum(r.jv) FROM l_u32_ms AS l ANY LEFT JOIN s_i16 AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- Wide integers decline: the narrowing is restricted to native integer widths (8/16/32/64), because
-- whether accurateCastOrNull is a pure domain check for Int128/UInt256 needs its own verification.
-- Today's refusal stands for a wide left key; that is a recorded residual, not a silent gap.
CREATE TABLE l_u128 (k UInt128) ENGINE = Memory;
INSERT INTO l_u128 VALUES (2), (4), (18446744073709551618);
SELECT sum(r.jv) FROM l_u128 AS l ANY LEFT JOIN s_u64 AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

-- 10. Wrapper matrix: every narrowing carrier is fixed AND keeps the direct lookup.
CREATE TABLE l_nu64 (k Nullable(UInt64)) ENGINE = Memory;
INSERT INTO l_nu64 VALUES (2), (4), (4294967298), (NULL);
CREATE TABLE s_u32b (k UInt32, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_u32b VALUES (2, 10), (4, 20);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_nu64 AS l ANY LEFT JOIN s_u32b AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_nu64 AS l ANY LEFT JOIN s_u32b AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64_oor AS l ANY LEFT JOIN s_nu32 AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_u64_oor AS l ANY LEFT JOIN s_nu32 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_nu64 AS l ANY LEFT JOIN s_nu32 AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_nu64 AS l ANY LEFT JOIN s_nu32 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
CREATE TABLE l_lcnu64 (k LowCardinality(Nullable(UInt64))) ENGINE = Memory;
INSERT INTO l_lcnu64 VALUES (2), (4), (4294967298), (NULL);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_lcnu64 AS l ANY LEFT JOIN s_u32b AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_lcnu64 AS l ANY LEFT JOIN s_u32b AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
-- The LowCardinality-left x Nullable-storage-key quadrant, against an ENGINE = Memory oracle.
CREATE TABLE o_nu32 (k Nullable(UInt32), jv Int64) ENGINE = Memory;
INSERT INTO o_nu32 VALUES (2, 10), (4, 20), (NULL, 99);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_lcnu64 AS l ANY LEFT JOIN o_nu32 AS r USING (k);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_lcnu64 AS l ANY LEFT JOIN s_nu32 AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_lcnu64 AS l ANY LEFT JOIN s_nu32 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
-- Same width, nullability only, but mirrored: a non-nullable left key against a nullable storage key.
-- Unlike the nullable-left direction pinned in section 8, this one has no working plan on master (the
-- storage key still gets a _CAST and is still refused), so the narrowing rewrite must fire here. The
-- one-sidedness of the first clause of canNarrowLeftKeyToStorageKey is what makes both true at once.
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u32 AS l ANY LEFT JOIN o_nu32 AS r USING (k);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u32 AS l ANY LEFT JOIN s_nu32 AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_u32 AS l ANY LEFT JOIN s_nu32 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
CREATE TABLE l_lcu32 (k LowCardinality(UInt32)) ENGINE = Memory;
INSERT INTO l_lcu32 VALUES (2), (4), (7);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_lcu32 AS l ANY LEFT JOIN o_nu32 AS r USING (k);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_lcu32 AS l ANY LEFT JOIN s_nu32 AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_lcu32 AS l ANY LEFT JOIN s_nu32 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';

-- 11. LowCardinality left key: the common type drops LowCardinality, so both sides of the
-- narrowing check must be compared through removeLowCardinalityAndNullable.
CREATE TABLE l_lcu64 (k LowCardinality(UInt64)) ENGINE = Memory;
INSERT INTO l_lcu64 VALUES (0), (2), (4), (4294967298);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_lcu64 AS l ANY LEFT JOIN s_u32b AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_lcu64 AS l ANY LEFT JOIN s_u32b AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_lcu64 AS l ANY LEFT JOIN o_nu32 AS r USING (k);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_lcu64 AS l ANY LEFT JOIN s_nu32 AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_lcu64 AS l ANY LEFT JOIN s_nu32 AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';

-- 12. Null-safe equals reaches the same conversion, so the narrowing applies there too, but only for a
-- non-nullable storage key. When the storage key is nullable as well, the null-safe rewrite wraps both
-- keys in tuple(), and a tuple expression is not a mapped storage column, so that shape stays refused.
-- Master refuses it too, for the same reason.
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64_oor AS l ANY LEFT JOIN o_u64 AS r ON l.k IS NOT DISTINCT FROM r.k;
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64_oor AS l ANY LEFT JOIN s_u32 AS r ON l.k IS NOT DISTINCT FROM r.k;
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_u64_oor AS l ANY LEFT JOIN s_u32 AS r ON l.k IS NOT DISTINCT FROM r.k)
WHERE explain ILIKE '%FilledJoin%';
SELECT sum(r.jv) FROM l_u64_oor AS l ANY LEFT JOIN s_nu32 AS r ON l.k IS NOT DISTINCT FROM r.k; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

-- 13. RIGHT and FULL USING with the bare USING column projected. There the output key comes from the
-- right side and a required right key is materialised from the left key column, whose type the
-- rewrite changes, so these shapes exercise a different path than the ANY LEFT cases above.
CREATE TABLE s_r32 (k UInt32, jv Int64) ENGINE = Join(ALL, RIGHT, k);
INSERT INTO s_r32 VALUES (2, 10), (4, 20);
CREATE TABLE o_r32 (k UInt32, jv Int64) ENGINE = Memory;
INSERT INTO o_r32 VALUES (2, 10), (4, 20);
CREATE TABLE l_r64 (k UInt64, lv String) ENGINE = Memory;
INSERT INTO l_r64 VALUES (2, 'a'), (4294967298, 'b');
SELECT arraySort(groupArray((k, jv, lv))) FROM l_r64 ALL RIGHT JOIN o_r32 USING (k);
SELECT arraySort(groupArray((k, jv, lv))) FROM l_r64 ALL RIGHT JOIN s_r32 USING (k);
-- The projected USING column keeps the left key's declared type, not the narrowed storage key type.
SELECT arraySort(groupArray(toTypeName(k))) FROM l_r64 ALL RIGHT JOIN s_r32 USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT k FROM l_r64 ALL RIGHT JOIN s_r32 USING (k))
WHERE explain ILIKE '%FilledJoin%';
CREATE TABLE s_f32 (k UInt32, jv Int64) ENGINE = Join(ALL, FULL, k);
INSERT INTO s_f32 VALUES (2, 10), (4, 20);
CREATE TABLE o_f32 (k UInt32, jv Int64) ENGINE = Memory;
INSERT INTO o_f32 VALUES (2, 10), (4, 20);
SELECT arraySort(groupArray((k, jv, lv))) FROM l_r64 ALL FULL JOIN o_f32 USING (k);
SELECT arraySort(groupArray((k, jv, lv))) FROM l_r64 ALL FULL JOIN s_f32 USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT k FROM l_r64 ALL FULL JOIN s_f32 USING (k))
WHERE explain ILIKE '%FilledJoin%';

-- 14. Strictness axis: the Join engine also accepts SEMI/ANTI, and a SEMI storage table reaches the
-- same conversion, so the narrowing must apply there too. Master refuses this shape.
CREATE TABLE s_semi (k UInt32, jv Int64) ENGINE = Join(SEMI, LEFT, k);
INSERT INTO s_semi VALUES (2, 10), (4, 20);
CREATE TABLE o_semi (k UInt32, jv Int64) ENGINE = Memory;
INSERT INTO o_semi VALUES (2, 10), (4, 20);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64_oor AS l SEMI LEFT JOIN o_semi AS r USING (k);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_u64_oor AS l SEMI LEFT JOIN s_semi AS r USING (k);
SELECT count() > 0 FROM (EXPLAIN PLAN description = 1
    SELECT sum(r.jv) FROM l_u64_oor AS l SEMI LEFT JOIN s_semi AS r USING (k))
WHERE explain ILIKE '%FilledJoin%';

-- 15. Custom-named integer storage keys decline: Bool is a UInt8 whose comparison semantics differ,
-- accurateCastOrNull(2, 'Bool') is true, so admitting it would report 14 / [7,7] instead of 7 / [7,0].
CREATE TABLE s_bool (k Bool, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_bool VALUES (true, 7);
CREATE TABLE l_i64_b (k Int64) ENGINE = Memory;
INSERT INTO l_i64_b VALUES (1), (2);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_i64_b AS l ANY LEFT JOIN s_bool AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
CREATE TABLE s_nbool (k Nullable(Bool), jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_nbool VALUES (true, 7);
CREATE TABLE l_i64_b3 (k Int64) ENGINE = Memory;
INSERT INTO l_i64_b3 VALUES (1), (2), (3);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_i64_b3 AS l ANY LEFT JOIN s_nbool AS r USING (k); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- a Bool left key against a UInt8 storage key is not a narrowing and works today
CREATE TABLE s_u8 (k UInt8, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO s_u8 VALUES (1, 7);
CREATE TABLE l_bool (k Bool) ENGINE = Memory;
INSERT INTO l_bool VALUES (false), (true);
SELECT sum(r.jv), arraySort(groupArray((l.k, r.jv))) FROM l_bool AS l ANY LEFT JOIN s_u8 AS r USING (k);

-- Leave nothing behind for the next run in the shared-database mode described above.
DROP TABLE IF EXISTS jt;
DROP TABLE IF EXISTS big;
DROP TABLE IF EXISTS s_u32;
DROP TABLE IF EXISTS o_u64;
DROP TABLE IF EXISTS l_u64;
DROP TABLE IF EXISTS l_i64;
DROP TABLE IF EXISTS s_nu32;
DROP TABLE IF EXISTS l_u64_oor;
DROP TABLE IF EXISTS s_mk;
DROP TABLE IF EXISTS l_mk;
DROP TABLE IF EXISTS o_mk;
DROP TABLE IF EXISTS s_u64;
DROP TABLE IF EXISTS l_u32;
DROP TABLE IF EXISTS l_nu32;
DROP TABLE IF EXISTS s_rj;
DROP TABLE IF EXISTS l_rj;
DROP TABLE IF EXISTS s_str;
DROP TABLE IF EXISTS l_fs;
DROP TABLE IF EXISTS l_f64;
DROP TABLE IF EXISTS s_i32;
DROP TABLE IF EXISTS l_dec;
DROP TABLE IF EXISTS s_dt;
DROP TABLE IF EXISTS l_dt64;
DROP TABLE IF EXISTS s_lc;
DROP TABLE IF EXISTS l_u32_lc;
DROP TABLE IF EXISTS s_i16;
DROP TABLE IF EXISTS l_u32_ms;
DROP TABLE IF EXISTS l_u128;
DROP TABLE IF EXISTS l_nu64;
DROP TABLE IF EXISTS s_u32b;
DROP TABLE IF EXISTS l_lcnu64;
DROP TABLE IF EXISTS o_nu32;
DROP TABLE IF EXISTS l_lcu32;
DROP TABLE IF EXISTS l_lcu64;
DROP TABLE IF EXISTS s_r32;
DROP TABLE IF EXISTS o_r32;
DROP TABLE IF EXISTS l_r64;
DROP TABLE IF EXISTS s_f32;
DROP TABLE IF EXISTS o_f32;
DROP TABLE IF EXISTS s_semi;
DROP TABLE IF EXISTS o_semi;
DROP TABLE IF EXISTS s_bool;
DROP TABLE IF EXISTS l_i64_b;
DROP TABLE IF EXISTS s_nbool;
DROP TABLE IF EXISTS l_i64_b3;
DROP TABLE IF EXISTS s_u8;
DROP TABLE IF EXISTS l_bool;
