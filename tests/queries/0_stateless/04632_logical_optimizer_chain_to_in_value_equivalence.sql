-- The chain -> IN/NOT IN rewrites (and the 2-term AND fold) may only fire when the constant,
-- converted into the expression's type, is a faithful semantic point: exact equality on the converted
-- value has to agree with what `equals` computes. Where it does not, the rewrite silently changes
-- results, or fails a query that works without it.
--
-- Every "on" row below is printed next to ground truth computed as one subquery column per
-- comparison, so the OR/AND is over plain UInt8 columns and no equals-chain pattern is left for the
-- pass to rewrite. Ground truth is used throughout the file, and not just where a rewrite-OFF column
-- would be awkward: `rewrite OFF` is not a usable fallback at all, because both builders bypass the
-- chain-length gate for a LowCardinality expression, so an OFF column would itself be wrong there.

-- The pass is analyzer-only; without this the assertions are vacuous under the old-analyzer jobs.
SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;

-- =====================================================================================
-- (a) Correctness: every row must equal its ground truth.
-- =====================================================================================

SELECT '--- float zero / NaN ---';

DROP TABLE IF EXISTS t_f64;
CREATE TABLE t_f64 (f Float64) ENGINE = Memory;
INSERT INTO t_f64 VALUES (-0.0), (1.0), (2.0), (3.0);

SELECT countIf(f = 0.0 OR f = 1.0 OR f = 2.0) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT f = 0.0 AS g0, f = 1.0 AS g1, f = 2.0 AS g2 FROM t_f64)) AS ground_truth FROM t_f64;
SELECT countIf(f != 0.0 AND f != 1.0 AND f != 2.0) AS res, (SELECT countIf(g0 AND g1 AND g2) FROM (SELECT f != 0.0 AS g0, f != 1.0 AS g1, f != 2.0 AS g2 FROM t_f64)) AS ground_truth FROM t_f64;

-- The zero can arrive from a non-float constant, so the check has to run on the CONVERTED value.
SELECT countIf(f = 0 OR f = 1 OR f = 2) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT f = 0 AS g0, f = 1 AS g1, f = 2 AS g2 FROM t_f64)) AS ground_truth FROM t_f64;

DROP TABLE IF EXISTS t_f32;
CREATE TABLE t_f32 (f Float32) ENGINE = Memory;
INSERT INTO t_f32 VALUES (-0.0), (1.0), (2.0), (3.0);
SELECT countIf(f = toFloat32(0.0) OR f = toFloat32(1.0) OR f = toFloat32(2.0)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT f = toFloat32(0.0) AS g0, f = toFloat32(1.0) AS g1, f = toFloat32(2.0) AS g2 FROM t_f32)) AS ground_truth FROM t_f32;

DROP TABLE IF EXISTS t_nan;
CREATE TABLE t_nan (f Float64) ENGINE = Memory;
INSERT INTO t_nan VALUES (nan), (1.0), (2.0), (3.0);
SELECT countIf(f = nan OR f = 1.0 OR f = 2.0) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT f = nan AS g0, f = 1.0 AS g1, f = 2.0 AS g2 FROM t_nan)) AS ground_truth FROM t_nan;
SELECT countIf(f != nan AND f != 1.0 AND f != 2.0) AS res, (SELECT countIf(g0 AND g1 AND g2) FROM (SELECT f != nan AS g0, f != 1.0 AS g1, f != 2.0 AS g2 FROM t_nan)) AS ground_truth FROM t_nan;
-- A String constant converts into NaN, which a raw-Field float test cannot see.
SELECT countIf(f = 'nan' OR f = '1' OR f = '2') AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT f = 'nan' AS g0, f = '1' AS g1, f = '2' AS g2 FROM t_nan)) AS ground_truth FROM t_nan;

SELECT '--- float zero, wrapped expressions ---';

DROP TABLE IF EXISTS t_lcf;
CREATE TABLE t_lcf (f LowCardinality(Float64)) ENGINE = Memory;
INSERT INTO t_lcf VALUES (-0.0), (1.0), (2.0), (3.0);
SELECT countIf(f = 0.0 OR f = 1.0 OR f = 2.0) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT f = 0.0 AS g0, f = 1.0 AS g1, f = 2.0 AS g2 FROM t_lcf)) AS ground_truth FROM t_lcf;

DROP TABLE IF EXISTS t_nf;
CREATE TABLE t_nf (f Nullable(Float64)) ENGINE = Memory;
INSERT INTO t_nf VALUES (-0.0), (1.0), (2.0), (3.0);
SELECT countIf(f = 0.0 OR f = 1.0 OR f = 2.0) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT f = 0.0 AS g0, f = 1.0 AS g1, f = 2.0 AS g2 FROM t_nf)) AS ground_truth FROM t_nf;

DROP TABLE IF EXISTS t_af;
CREATE TABLE t_af (c Array(Float64)) ENGINE = Memory;
INSERT INTO t_af VALUES ([-0.0]), ([1.0]), ([2.0]), ([3.0]);
SELECT countIf(c = [0.0] OR c = [1.0] OR c = [2.0]) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = [0.0] AS g0, c = [1.0] AS g1, c = [2.0] AS g2 FROM t_af)) AS ground_truth FROM t_af;
SELECT countIf(c != [0.0] AND c != [1.0] AND c != [2.0]) AS res, (SELECT countIf(g0 AND g1 AND g2) FROM (SELECT c != [0.0] AS g0, c != [1.0] AS g1, c != [2.0] AS g2 FROM t_af)) AS ground_truth FROM t_af;

DROP TABLE IF EXISTS t_tf;
CREATE TABLE t_tf (c Tuple(Float64)) ENGINE = Memory;
INSERT INTO t_tf VALUES (tuple(-0.0)), (tuple(1.0)), (tuple(2.0)), (tuple(3.0));
SELECT countIf(c = tuple(0.0) OR c = tuple(1.0) OR c = tuple(2.0)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = tuple(0.0) AS g0, c = tuple(1.0) AS g1, c = tuple(2.0) AS g2 FROM t_tf)) AS ground_truth FROM t_tf;

DROP TABLE IF EXISTS t_mf;
CREATE TABLE t_mf (c Map(String, Float64)) ENGINE = Memory;
INSERT INTO t_mf VALUES (map('k', -0.0)), (map('k', 1.0)), (map('k', 2.0)), (map('k', 3.0));
SELECT countIf(c = map('k', 0.0) OR c = map('k', 1.0) OR c = map('k', 2.0)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = map('k', 0.0) AS g0, c = map('k', 1.0) AS g1, c = map('k', 2.0) AS g2 FROM t_mf)) AS ground_truth FROM t_mf;

SELECT '--- nested NaN ---';

DROP TABLE IF EXISTS t_tnan;
CREATE TABLE t_tnan (c Tuple(Float64)) ENGINE = Memory;
INSERT INTO t_tnan VALUES (tuple(1.0)), (tuple(2.0)), (tuple(nan)), (tuple(3.0));
SELECT countIf(c = tuple(nan) OR c = tuple(1.0) OR c = tuple(2.0)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = tuple(nan) AS g0, c = tuple(1.0) AS g1, c = tuple(2.0) AS g2 FROM t_tnan)) AS ground_truth FROM t_tnan;
SELECT countIf(c != tuple(nan) AND c != tuple(1.0) AND c != tuple(2.0)) AS res, (SELECT countIf(g0 AND g1 AND g2) FROM (SELECT c != tuple(nan) AS g0, c != tuple(1.0) AS g1, c != tuple(2.0) AS g2 FROM t_tnan)) AS ground_truth FROM t_tnan;

-- Two DIFFERENT NaN payloads: container equality treats any two NaNs as equal, while a compound set
-- key hashes the nested float's raw bytes. `-nan` is normalised to `nan`'s payload, so the payloads
-- must be built with `reinterpret` (a 0x... UInt64 literal does not parse).
DROP TABLE IF EXISTS t_anan;
CREATE TABLE t_anan (c Array(Float64)) ENGINE = Memory;
INSERT INTO t_anan VALUES ([reinterpret(9221120237041090561::UInt64, 'Float64')]), ([1.0]), ([2.0]);
SELECT countIf(c = [reinterpret(9221120237041090562::UInt64, 'Float64')] OR c = [1.0] OR c = [2.0]) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = [reinterpret(9221120237041090562::UInt64, 'Float64')] AS g0, c = [1.0] AS g1, c = [2.0] AS g2 FROM t_anan)) AS ground_truth FROM t_anan;

DROP TABLE IF EXISTS t_mnan;
CREATE TABLE t_mnan (c Map(String, Float64)) ENGINE = Memory;
INSERT INTO t_mnan VALUES (map('k', reinterpret(9221120237041090561::UInt64, 'Float64'))), (map('k', 1.0)), (map('k', 2.0));
SELECT countIf(c = map('k', reinterpret(9221120237041090562::UInt64, 'Float64')) OR c = map('k', 1.0) OR c = map('k', 2.0)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = map('k', reinterpret(9221120237041090562::UInt64, 'Float64')) AS g0, c = map('k', 1.0) AS g1, c = map('k', 2.0) AS g2 FROM t_mnan)) AS ground_truth FROM t_mnan;

SELECT '--- FixedString padding vs a String target ---';

DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (c String) ENGINE = Memory;
INSERT INTO t_str VALUES ('ab'), ('cd'), ('ef'), ('zz');
SELECT countIf(c = toFixedString('ab', 5) OR c = toFixedString('cd', 5) OR c = toFixedString('ef', 5)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('ab', 5) AS g0, c = toFixedString('cd', 5) AS g1, c = toFixedString('ef', 5) AS g2 FROM t_str)) AS ground_truth FROM t_str;
SELECT countIf(c != toFixedString('ab', 5) AND c != toFixedString('cd', 5) AND c != toFixedString('ef', 5)) AS res, (SELECT countIf(g0 AND g1 AND g2) FROM (SELECT c != toFixedString('ab', 5) AS g0, c != toFixedString('cd', 5) AS g1, c != toFixedString('ef', 5) AS g2 FROM t_str)) AS ground_truth FROM t_str;

-- A FULL-WIDTH FixedString constant is unsound too: zero-padded comparison matches the value with
-- trailing NULs as well, while the set holds only the unpadded bytes.
DROP TABLE IF EXISTS t_strfull;
CREATE TABLE t_strfull (c String) ENGINE = Memory;
INSERT INTO t_strfull VALUES ('abcde'), ('abcde\0'), ('fghij'), ('zz');
SELECT countIf(c = toFixedString('abcde', 5) OR c = toFixedString('fghij', 5) OR c = toFixedString('zz', 5)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('abcde', 5) AS g0, c = toFixedString('fghij', 5) AS g1, c = toFixedString('zz', 5) AS g2 FROM t_strfull)) AS ground_truth FROM t_strfull;

DROP TABLE IF EXISTS t_lcstr;
CREATE TABLE t_lcstr (c LowCardinality(String)) ENGINE = Memory;
INSERT INTO t_lcstr VALUES ('ab'), ('cd'), ('ef'), ('zz');
SELECT countIf(c = toFixedString('ab', 5) OR c = toFixedString('cd', 5) OR c = toFixedString('ef', 5)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('ab', 5) AS g0, c = toFixedString('cd', 5) AS g1, c = toFixedString('ef', 5) AS g2 FROM t_lcstr)) AS ground_truth FROM t_lcstr;

-- The constant's own wrappers have to be stripped as well.
SELECT countIf(c = toFixedString('ab', 5)::Nullable(FixedString(5)) OR c = toFixedString('cd', 5)::Nullable(FixedString(5)) OR c = toFixedString('ef', 5)::Nullable(FixedString(5))) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('ab', 5)::Nullable(FixedString(5)) AS g0, c = toFixedString('cd', 5)::Nullable(FixedString(5)) AS g1, c = toFixedString('ef', 5)::Nullable(FixedString(5)) AS g2 FROM t_str)) AS ground_truth FROM t_str;
SELECT countIf(c = toFixedString('ab', 5)::LowCardinality(FixedString(5)) OR c = toFixedString('cd', 5)::LowCardinality(FixedString(5)) OR c = toFixedString('ef', 5)::LowCardinality(FixedString(5))) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('ab', 5)::LowCardinality(FixedString(5)) AS g0, c = toFixedString('cd', 5)::LowCardinality(FixedString(5)) AS g1, c = toFixedString('ef', 5)::LowCardinality(FixedString(5)) AS g2 FROM t_str)) AS ground_truth FROM t_str;

DROP TABLE IF EXISTS t_astr;
CREATE TABLE t_astr (c Array(String)) ENGINE = Memory;
INSERT INTO t_astr VALUES (['ab']), (['cd']), (['ef']), (['zz']);
SELECT countIf(c = [toFixedString('ab', 5)] OR c = [toFixedString('cd', 5)] OR c = [toFixedString('ef', 5)]) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = [toFixedString('ab', 5)] AS g0, c = [toFixedString('cd', 5)] AS g1, c = [toFixedString('ef', 5)] AS g2 FROM t_astr)) AS ground_truth FROM t_astr;

DROP TABLE IF EXISTS t_tstr;
CREATE TABLE t_tstr (c Tuple(String)) ENGINE = Memory;
INSERT INTO t_tstr VALUES (tuple('ab')), (tuple('cd')), (tuple('ef')), (tuple('zz'));
SELECT countIf(c = tuple(toFixedString('ab', 5)) OR c = tuple(toFixedString('cd', 5)) OR c = tuple(toFixedString('ef', 5))) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = tuple(toFixedString('ab', 5)) AS g0, c = tuple(toFixedString('cd', 5)) AS g1, c = tuple(toFixedString('ef', 5)) AS g2 FROM t_tstr)) AS ground_truth FROM t_tstr;

SELECT '--- Enum label vs the raw integer ---';

DROP TABLE IF EXISTS t_estr;
CREATE TABLE t_estr (c String) ENGINE = Memory;
INSERT INTO t_estr VALUES ('a'), ('b'), ('c'), ('z');
SELECT countIf(c = 'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3) OR c = 'b'::Enum8('a' = 1, 'b' = 2, 'c' = 3) OR c = 'c'::Enum8('a' = 1, 'b' = 2, 'c' = 3)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g0, c = 'b'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g1, c = 'c'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g2 FROM t_estr)) AS ground_truth FROM t_estr;
SELECT countIf(c != 'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AND c != 'b'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AND c != 'c'::Enum8('a' = 1, 'b' = 2, 'c' = 3)) AS res, (SELECT countIf(g0 AND g1 AND g2) FROM (SELECT c != 'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g0, c != 'b'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g1, c != 'c'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g2 FROM t_estr)) AS ground_truth FROM t_estr;

DROP TABLE IF EXISTS t_efs;
CREATE TABLE t_efs (c FixedString(3)) ENGINE = Memory;
INSERT INTO t_efs VALUES ('a'), ('b'), ('c'), ('z');
SELECT countIf(c = 'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3) OR c = 'b'::Enum8('a' = 1, 'b' = 2, 'c' = 3) OR c = 'c'::Enum8('a' = 1, 'b' = 2, 'c' = 3)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g0, c = 'b'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g1, c = 'c'::Enum8('a' = 1, 'b' = 2, 'c' = 3) AS g2 FROM t_efs)) AS ground_truth FROM t_efs;

-- A separate fixture: an Enum-label chain over the FixedString fixture above would match no rows
-- at all and the assertion would be vacuous.
DROP TABLE IF EXISTS t_tenum;
CREATE TABLE t_tenum (c Tuple(String)) ENGINE = Memory;
INSERT INTO t_tenum VALUES (tuple('a')), (tuple('b')), (tuple('c')), (tuple('z'));
SELECT countIf(c = tuple('a'::Enum8('a' = 1, 'b' = 2, 'c' = 3)) OR c = tuple('b'::Enum8('a' = 1, 'b' = 2, 'c' = 3)) OR c = tuple('c'::Enum8('a' = 1, 'b' = 2, 'c' = 3))) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = tuple('a'::Enum8('a' = 1, 'b' = 2, 'c' = 3)) AS g0, c = tuple('b'::Enum8('a' = 1, 'b' = 2, 'c' = 3)) AS g1, c = tuple('c'::Enum8('a' = 1, 'b' = 2, 'c' = 3)) AS g2 FROM t_tenum)) AS ground_truth FROM t_tenum;

SELECT '--- Variant / Dynamic expression and constant ---';

-- A Variant EXPRESSION: `equals` runs per active alternative, the set holds one discriminator-bearing
-- key, and the constant carries no discriminator at all.
DROP TABLE IF EXISTS t_var;
CREATE TABLE t_var (c Variant(Float64, UInt64)) ENGINE = Memory;
INSERT INTO t_var VALUES (1::Float64), (2::Float64), (3::Float64), (1::UInt64), (2::UInt64), (3::UInt64);
SELECT countIf(c = 1 OR c = 2 OR c = 3) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 1 AS g0, c = 2 AS g1, c = 3 AS g2 FROM t_var)) AS ground_truth FROM t_var;

DROP TABLE IF EXISTS t_tvar;
CREATE TABLE t_tvar (c Tuple(Variant(Float64, UInt64))) ENGINE = Memory;
INSERT INTO t_tvar VALUES (tuple(1::Float64)), (tuple(2::Float64)), (tuple(3::Float64)), (tuple(1::UInt64)), (tuple(2::UInt64));
SELECT countIf(c = tuple(1) OR c = tuple(2) OR c = tuple(3)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = tuple(1) AS g0, c = tuple(2) AS g1, c = tuple(3) AS g2 FROM t_tvar)) AS ground_truth FROM t_tvar;

-- A Variant CONSTANT hides its active alternative's real type behind a bare Field, so a source-type
-- check sees only `Variant` while `equals` uses the alternative's Enum / padded-FixedString semantics.
SELECT countIf(c = 'a'::Variant(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64) OR c = 'b'::Variant(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64) OR c = 'c'::Variant(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'a'::Variant(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64) AS g0, c = 'b'::Variant(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64) AS g1, c = 'c'::Variant(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64) AS g2 FROM t_estr)) AS ground_truth FROM t_estr;
SELECT countIf(c = toFixedString('ab', 5)::Variant(FixedString(5), UInt64) OR c = toFixedString('cd', 5)::Variant(FixedString(5), UInt64) OR c = toFixedString('ef', 5)::Variant(FixedString(5), UInt64)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('ab', 5)::Variant(FixedString(5), UInt64) AS g0, c = toFixedString('cd', 5)::Variant(FixedString(5), UInt64) AS g1, c = toFixedString('ef', 5)::Variant(FixedString(5), UInt64) AS g2 FROM t_str)) AS ground_truth FROM t_str;

-- `Dynamic` is the exact sibling; note the pre-existing `hasDynamicStructure()` bail-out inspects only
-- the EXPRESSION, so a Dynamic CONSTANT sails past it.
SELECT countIf(c = 'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3)::Dynamic OR c = 'b'::Enum8('a' = 1, 'b' = 2, 'c' = 3)::Dynamic OR c = 'c'::Enum8('a' = 1, 'b' = 2, 'c' = 3)::Dynamic) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3)::Dynamic AS g0, c = 'b'::Enum8('a' = 1, 'b' = 2, 'c' = 3)::Dynamic AS g1, c = 'c'::Enum8('a' = 1, 'b' = 2, 'c' = 3)::Dynamic AS g2 FROM t_estr)) AS ground_truth FROM t_estr;
SELECT countIf(c = toFixedString('ab', 5)::Dynamic OR c = toFixedString('cd', 5)::Dynamic OR c = toFixedString('ef', 5)::Dynamic) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('ab', 5)::Dynamic AS g0, c = toFixedString('cd', 5)::Dynamic AS g1, c = toFixedString('ef', 5)::Dynamic AS g2 FROM t_str)) AS ground_truth FROM t_str;

-- A nested lossy leaf conversion escapes the top-level round-trip guard. The `Tuple`/`Array`/`Map`
-- form of this is being fixed in `convertFieldToType` itself (PR #111399); only the `Variant`-source
-- form is in scope here. Small in-range values are load-bearing: a large `DateTime` cannot convert
-- into `Date` at all, so the chain would decline for an unrelated reason and the row would be vacuous.
DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (c Date) ENGINE = Memory;
INSERT INTO t_date VALUES (toDate(100)), (toDate(200)), (toDate(300)), (toDate(400));
SELECT countIf(c = toDateTime(100)::Variant(DateTime, UInt64) OR c = toDateTime(200)::Variant(DateTime, UInt64) OR c = toDateTime(300)::Variant(DateTime, UInt64)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toDateTime(100)::Variant(DateTime, UInt64) AS g0, c = toDateTime(200)::Variant(DateTime, UInt64) AS g1, c = toDateTime(300)::Variant(DateTime, UInt64) AS g2 FROM t_date)) AS ground_truth FROM t_date;

-- A SINGLE-alternative Variant expression is just as unsound: the shared semantic-point rule cannot
-- see through a Variant TARGET (it matches none of its string / Tuple / Array / Map branches), so the
-- alternative count is not what makes the shape safe and the decline has to key on the type.
DROP TABLE IF EXISTS t_var1s;
CREATE TABLE t_var1s (c Variant(String)) ENGINE = Memory;
INSERT INTO t_var1s VALUES ('ab'), ('cd'), ('ef'), ('zz');
SELECT countIf(c = toFixedString('ab', 5) OR c = toFixedString('cd', 5) OR c = toFixedString('ef', 5)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('ab', 5) AS g0, c = toFixedString('cd', 5) AS g1, c = toFixedString('ef', 5) AS g2 FROM t_var1s)) AS ground_truth FROM t_var1s;
SELECT countIf(c = 'ab'::Enum8('ab' = 1, 'cd' = 2, 'ef' = 3) OR c = 'cd'::Enum8('ab' = 1, 'cd' = 2, 'ef' = 3) OR c = 'ef'::Enum8('ab' = 1, 'cd' = 2, 'ef' = 3)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'ab'::Enum8('ab' = 1, 'cd' = 2, 'ef' = 3) AS g0, c = 'cd'::Enum8('ab' = 1, 'cd' = 2, 'ef' = 3) AS g1, c = 'ef'::Enum8('ab' = 1, 'cd' = 2, 'ef' = 3) AS g2 FROM t_var1s)) AS ground_truth FROM t_var1s;

DROP TABLE IF EXISTS t_tvar1s;
CREATE TABLE t_tvar1s (c Tuple(Variant(String))) ENGINE = Memory;
INSERT INTO t_tvar1s VALUES (tuple('ab')), (tuple('cd')), (tuple('zz'));
SELECT countIf(c = tuple(toFixedString('ab', 5)) OR c = tuple(toFixedString('cd', 5)) OR c = tuple(toFixedString('ef', 5))) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = tuple(toFixedString('ab', 5)) AS g0, c = tuple(toFixedString('cd', 5)) AS g1, c = tuple(toFixedString('ef', 5)) AS g2 FROM t_tvar1s)) AS ground_truth FROM t_tvar1s;

-- =====================================================================================
-- (b) The optimization must still FIRE wherever the converted value IS a semantic point.
--     A blanket exclusion (e.g. rejecting all floats by type) makes these drop to 0.
-- =====================================================================================

SELECT '--- still rewritten ---';

SELECT 'float nonzero in', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_f64 WHERE f = 1.5 OR f = 2.5 OR f = 3.5) WHERE explain ILIKE '%function_name: in%';
SELECT 'float nonzero notIn', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_f64 WHERE f != 1.5 AND f != 2.5 AND f != 3.5) WHERE explain ILIKE '%function_name: notIn%';
SELECT 'float inf', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_f64 WHERE f = inf OR f = -inf OR f = 1.5) WHERE explain ILIKE '%function_name: in%';
SELECT 'float cross-width', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_f64 WHERE f = toFloat32(1.5) OR f = toFloat32(2.5) OR f = toFloat32(3.5)) WHERE explain ILIKE '%function_name: in%';
-- Rows containing -0.0/NaN are irrelevant: only the CONSTANTS matter.
SELECT 'float rows with -0.0', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_f64 WHERE f = 1.5 OR f = 2.5 OR f = 3.5) WHERE explain ILIKE '%function_name: in%';
SELECT 'array float nonzero', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_af WHERE c = [1.5] OR c = [2.5] OR c = [3.5]) WHERE explain ILIKE '%function_name: in%';
SELECT 'tuple float nonzero', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_tf WHERE c = tuple(1.5) OR c = tuple(2.5) OR c = tuple(3.5)) WHERE explain ILIKE '%function_name: in%';
SELECT 'map float nonzero', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_mf WHERE c = map('k', 1.5) OR c = map('k', 2.5) OR c = map('k', 3.5)) WHERE explain ILIKE '%function_name: in%';
-- A NaN below an Array boundary must NOT reach a set (the set hashes the raw payload while
-- container equality treats any two NaNs as equal), so this row is expected to be DECLINED. The
-- paired Tuple(Float64) NaN fold in section (d) shows the same value staying foldable for an AND.
SELECT 'array NaN declined for the set', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_af WHERE c = [nan] OR c = [1.5] OR c = [2.5]) WHERE explain ILIKE '%function_name: in%';

SELECT 'string constants', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_str WHERE c = 'ab' OR c = 'cd' OR c = 'ef') WHERE explain ILIKE '%function_name: in%';
SELECT 'string constants notIn', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_str WHERE c != 'ab' AND c != 'cd' AND c != 'ef') WHERE explain ILIKE '%function_name: notIn%';

DROP TABLE IF EXISTS t_fs5;
CREATE TABLE t_fs5 (c FixedString(5)) ENGINE = Memory;
INSERT INTO t_fs5 VALUES ('ab'), ('cd'), ('ef');
-- A FixedString(N) constant against a wider-or-equal FixedString(M) target pads into exactly one key.
SELECT 'fs5 expr fs5 const', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_fs5 WHERE c = toFixedString('ab', 5) OR c = toFixedString('cd', 5) OR c = toFixedString('ef', 5)) WHERE explain ILIKE '%function_name: in%';
SELECT 'fs5 expr fs3 const', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_fs5 WHERE c = toFixedString('ab', 3) OR c = toFixedString('cd', 3) OR c = toFixedString('ef', 3)) WHERE explain ILIKE '%function_name: in%';
SELECT 'fs5 expr short string const', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_fs5 WHERE c = 'ab' OR c = 'cd' OR c = 'ef') WHERE explain ILIKE '%function_name: in%';
SELECT 'fs5 expr string const with NUL', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_fs5 WHERE c = 'ab\0' OR c = 'cd\0' OR c = 'ef\0') WHERE explain ILIKE '%function_name: in%';

-- The reverse Enum direction agrees and must keep firing.
DROP TABLE IF EXISTS t_enum;
CREATE TABLE t_enum (c Enum8('a' = 1, 'b' = 2, 'c' = 3, 'z' = 4)) ENGINE = Memory;
INSERT INTO t_enum VALUES ('a'), ('b'), ('c'), ('z');
SELECT 'enum expr string const', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_enum WHERE c = 'a' OR c = 'b' OR c = 'c') WHERE explain ILIKE '%function_name: in%';
SELECT 'enum expr int const', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_enum WHERE c = 1::Int8 OR c = 2::Int8 OR c = 3::Int8) WHERE explain ILIKE '%function_name: in%';

DROP TABLE IF EXISTS t_dec;
CREATE TABLE t_dec (c Decimal(9, 1)) ENGINE = Memory;
INSERT INTO t_dec VALUES (1.1), (2.2), (3.3);
SELECT 'decimal scales', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_dec WHERE c = 1.10::Decimal(9, 2) OR c = 2.20::Decimal(9, 2) OR c = 3.30::Decimal(9, 2)) WHERE explain ILIKE '%function_name: in%';

SELECT 'date string const', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_date WHERE c = '1970-04-11' OR c = '1970-07-20' OR c = '1970-10-28') WHERE explain ILIKE '%function_name: in%';

DROP TABLE IF EXISTS t_u8;
CREATE TABLE t_u8 (c UInt8) ENGINE = Memory;
INSERT INTO t_u8 VALUES (1), (2), (3);
SELECT 'integer widths', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_u8 WHERE c = 1::Int64 OR c = 2::Int64 OR c = 3::Int64) WHERE explain ILIKE '%function_name: in%';

SELECT 'lowcardinality string', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_lcstr WHERE c = 'ab' OR c = 'cd' OR c = 'ef') WHERE explain ILIKE '%function_name: in%';

DROP TABLE IF EXISTS t_uuid;
CREATE TABLE t_uuid (c UUID) ENGINE = Memory;
INSERT INTO t_uuid VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0000-0000-000000000002'), ('00000000-0000-0000-0000-000000000003');
SELECT 'uuid string const', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_uuid WHERE c = '00000000-0000-0000-0000-000000000001' OR c = '00000000-0000-0000-0000-000000000002' OR c = '00000000-0000-0000-0000-000000000003') WHERE explain ILIKE '%function_name: in%';

-- The AND builder deliberately has NO Variant-expression rule. This shape is already declined
-- upstream (the resulting notIn would be Nullable where the notEquals chain was not), on both an
-- unfixed and a fixed build, so a 0 here pins that no rule was added rather than a lost rewrite.
SELECT 'variant expr notIn declined upstream', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_var WHERE c != 1 AND c != 2 AND c != 3) WHERE explain ILIKE '%function_name: notIn%';

-- The measured COST of declining Variant expressions by type: these three are correct today, so their
-- results must stay right while the rewrite is forgone. They are asserted as `fired = 0` deliberately -
-- a `1` here would mean the decline had been narrowed and the carriers above are live again.
DROP TABLE IF EXISTS t_vf32;
CREATE TABLE t_vf32 (c Variant(Float32)) ENGINE = Memory;
INSERT INTO t_vf32 VALUES (1.5), (2.5), (3.5), (9.5);
SELECT 'variant float32 forgone', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_vf32 WHERE c = toFloat64(1.5) OR c = toFloat64(2.5) OR c = toFloat64(3.5)) WHERE explain ILIKE '%function_name: in%';
SELECT countIf(c = toFloat64(1.5) OR c = toFloat64(2.5) OR c = toFloat64(3.5)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFloat64(1.5) AS g0, c = toFloat64(2.5) AS g1, c = toFloat64(3.5) AS g2 FROM t_vf32)) AS ground_truth FROM t_vf32;

DROP TABLE IF EXISTS t_venum;
CREATE TABLE t_venum (c Variant(Enum8('a' = 1, 'b' = 2, 'c' = 3, 'z' = 9))) ENGINE = Memory;
INSERT INTO t_venum VALUES ('a'), ('b'), ('c'), ('z');
SELECT 'variant enum forgone', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_venum WHERE c = 'a' OR c = 'b' OR c = 'c') WHERE explain ILIKE '%function_name: in%';
SELECT countIf(c = 'a' OR c = 'b' OR c = 'c') AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'a' AS g0, c = 'b' AS g1, c = 'c' AS g2 FROM t_venum)) AS ground_truth FROM t_venum;

DROP TABLE IF EXISTS t_vu256;
CREATE TABLE t_vu256 (c Variant(UInt256)) ENGINE = Memory;
INSERT INTO t_vu256 VALUES (1), (2), (3), (9);
SELECT 'variant uint256 forgone', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_vu256 WHERE c = toInt64(1) OR c = toInt64(2) OR c = toInt64(3)) WHERE explain ILIKE '%function_name: in%';
SELECT countIf(c = toInt64(1) OR c = toInt64(2) OR c = toInt64(3)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toInt64(1) AS g0, c = toInt64(2) AS g1, c = toInt64(3) AS g2 FROM t_vu256)) AS ground_truth FROM t_vu256;

-- =====================================================================================
-- (d) The 2-term AND fold shares the root cause but has its OWN rule partition: NaN is
--     shared with the set builders, the float ZERO test is deliberately NOT.
-- =====================================================================================

SELECT '--- 2-term AND fold ---';

DROP TABLE IF EXISTS t_fold;
CREATE TABLE t_fold (c String) ENGINE = Memory;
INSERT INTO t_fold VALUES ('a'), ('b'), ('1'), ('z');
SELECT countIf(c = 'a'::Enum8('a' = 1, 'b' = 2) AND c != '1') AS res, (SELECT countIf(g0 AND g1) FROM (SELECT c = 'a'::Enum8('a' = 1, 'b' = 2) AS g0, c != '1' AS g1 FROM t_fold)) AS ground_truth FROM t_fold;
SELECT countIf(c = toFixedString('a', 3) AND c != 'a') AS res, (SELECT countIf(g0 AND g1) FROM (SELECT c = toFixedString('a', 3) AS g0, c != 'a' AS g1 FROM t_fold)) AS ground_truth FROM t_fold;

-- Pure size mismatch, no Enum involved: this is why the size rule is shared and not set-specific.
DROP TABLE IF EXISTS t_foldfs;
CREATE TABLE t_foldfs (c FixedString(3)) ENGINE = Memory;
INSERT INTO t_foldfs VALUES ('ab'), ('cd'), ('ef');
SELECT countIf(c = toFixedString('ab', 5) AND c != 'ab') AS res, (SELECT countIf(g0 AND g1) FROM (SELECT c = toFixedString('ab', 5) AS g0, c != 'ab' AS g1 FROM t_foldfs)) AS ground_truth FROM t_foldfs;
SELECT countIf(c != 'ab' AND c = toFixedString('ab', 5)) AS res, (SELECT countIf(g0 AND g1) FROM (SELECT c != 'ab' AS g0, c = toFixedString('ab', 5) AS g1 FROM t_foldfs)) AS ground_truth FROM t_foldfs;

DROP TABLE IF EXISTS t_afnan;
CREATE TABLE t_afnan (c Array(Float64)) ENGINE = Memory;
INSERT INTO t_afnan VALUES ([1.0]), ([2.0]), ([nan]), ([3.0]);
DROP TABLE IF EXISTS t_mfnan;
CREATE TABLE t_mfnan (c Map(String, Float64)) ENGINE = Memory;
INSERT INTO t_mfnan VALUES (map('k', 1.0)), (map('k', 2.0)), (map('k', nan)), (map('k', 3.0));

-- A nested NaN is unorderable under execution's scalar semantics: this is why the NaN rule is shared.
SELECT countIf(c = tuple(1.0) AND c < tuple(nan)) AS res, (SELECT countIf(g0 AND g1) FROM (SELECT c = tuple(1.0) AS g0, c < tuple(nan) AS g1 FROM t_tnan)) AS ground_truth FROM t_tnan;

-- A NaN below an Array/Map boundary must stay foldable: there both sides of the comparison go
-- through the same `compareAt`, which treats any two NaNs as equal, so the semantics already match
-- and recursing the fold's NaN check deeper would only lose valid folds. This is the pair to the
-- `Tuple(Float64)` row above, and it is what pins the per-consumer depth.
SELECT countIf(c = [1.0] AND c < [nan]) AS res, (SELECT countIf(g0 AND g1) FROM (SELECT c = [1.0] AS g0, c < [nan] AS g1 FROM t_afnan)) AS ground_truth FROM t_afnan;
SELECT countIf(c = map('k', 1.0) AND c < map('k', nan)) AS res, (SELECT countIf(g0 AND g1) FROM (SELECT c = map('k', 1.0) AS g0, c < map('k', nan) AS g1 FROM t_mfnan)) AS ground_truth FROM t_mfnan;
-- The RESULT is the same either way here, so assert the PLAN SHAPE: the fold must still collapse the
-- two terms into one. A deeper NaN check in the fold leaves the `and` in place, which is the
-- (silent, results-correct) performance regression this pins.
SELECT 'array NaN fold still collapses', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_afnan WHERE c = [1.0] AND c < [nan]) WHERE explain ILIKE '%function_name: and%';
SELECT 'map NaN fold still collapses', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_mfnan WHERE c = map('k', 1.0) AND c < map('k', nan)) WHERE explain ILIKE '%function_name: and%';

-- Nested float ZERO must be UNCHANGED: the fold compares with accurateEquals, under which
-- `+0.0 == -0.0`, so these are correct already and routing them aside would lose a valid fold.
SELECT countIf(c = tuple(0.0) AND c != tuple(0.0)) AS zero_fold_1 FROM t_tf;
SELECT countIf(c = tuple(-0.0) AND c != tuple(0.0)) AS zero_fold_2 FROM t_tf;
SELECT countIf(f = 0.0 AND f != 0.0) AS zero_fold_3 FROM t_f64;
SELECT countIf(f = -0.0 AND f != 0.0) AS zero_fold_4 FROM t_f64;
SELECT countIf(f = 0.0 AND f != -0.0) AS zero_fold_5 FROM t_f64;
SELECT countIf(f >= 0.0 AND f <= 0.0) AS zero_range FROM t_f64;

-- The fold must not be disabled wholesale.
SELECT countIf(c = 3 AND c != 3) AS int_fold FROM t_u8;
SELECT 'int contradiction folded', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_u8 WHERE c = 3 AND c != 3) WHERE explain ILIKE '%constant_value: UInt64_0%';
SELECT 'float zero contradiction folded', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_f64 WHERE f = 0.0 AND f != 0.0) WHERE explain ILIKE '%constant_value: UInt64_0%';
-- ... while the Enum mis-fold is gone.
SELECT 'enum mis-fold gone', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_fold WHERE c = 'a'::Enum8('a' = 1, 'b' = 2) AND c != '1') WHERE explain ILIKE '%constant_value: UInt64_0%';

-- A dynamic-structure expression has no fixed type to compare against, so the fold must not answer a
-- query whose own terms legitimately fail. Both set builders already decline these.
DROP TABLE IF EXISTS t_afnan;
DROP TABLE IF EXISTS t_mfnan;
DROP TABLE IF EXISTS t_json;
CREATE TABLE t_json (c JSON(x String)) ENGINE = Memory;
INSERT INTO t_json VALUES ('{"x":"a"}'), ('{"x":"b"}'), ('{"x":"1"}');
SELECT countIf(c = '{"x":"a"}'::JSON(x Enum8('a' = 1, 'b' = 2)) AND c != '{"x":1}'::JSON(x Int64)) FROM t_json; -- { serverError NO_COMMON_TYPE }

-- Two expressions, so a contradiction on the ORDINARY column reaches the global fold: it may only
-- collapse the whole AND to `false` if no parked filter is opaque. Routing the dynamic-structure term
-- aside with an EMPTY converted value is what vetoes that collapse and keeps the query's own failure
-- visible; without the veto the answer depends on `optimize_redundant_comparisons`.
DROP TABLE IF EXISTS t_jveto;
CREATE TABLE t_jveto (i Int32, c JSON(x String)) ENGINE = Memory;
INSERT INTO t_jveto VALUES (1, '{"x":"a"}'), (2, '{"x":"b"}');
SELECT countIf((i = 1) AND (i = 2) AND (c != '{"x":1}'::JSON(x Int64))) FROM t_jveto; -- { serverError NO_COMMON_TYPE }
-- ... and the veto's breadth is measured rather than assumed: to be able to veto at all it has to park
-- before conversion with no converted value, so it keys on the expression TYPE and a SAFE comparison
-- over the same JSON column is vetoed too, retaining that AND as well. That is a disclosed cost, not a
-- wrong answer: the contradiction is still false at runtime, only the fold is forgone.
SELECT countIf((i = 1) AND (i = 2) AND (c != '{"x":"zz"}')) AS safe_result_unaffected FROM t_jveto;
SELECT 'safe json comparison is vetoed too', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_jveto WHERE (i = 1) AND (i = 2) AND (c != '{"x":"zz"}')) WHERE explain ILIKE '%constant_value: UInt64_0%';
-- ... while the same contradiction WITHOUT a dynamic-structure term still folds, so the veto is scoped
-- to the dynamic-structure case and the fold is not disabled wholesale.
SELECT 'plain contradiction still folds', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_jveto WHERE (i = 1) AND (i = 2)) WHERE explain ILIKE '%constant_value: UInt64_0%';

-- =====================================================================================
-- (e) No common type between the two comparison operands. `equals` on a string-family operand is
--     accepted at analysis time but is only EXECUTABLE when the const-string path can cast the
--     constant, which needs the CONSTANT to be the string side. A string-family expression against
--     e.g. an `Int64` constant therefore raises NO_COMMON_TYPE, while the constant still converts
--     losslessly into a semantic point, so all three rewrites used to answer instead. This holds for
--     composite constants as well, not just scalar ones. The reverse direction stays rewritable and is
--     asserted in section (b) (`date string const`, `uuid string const`, `fs5 expr string const`,
--     `enum expr string const`).
-- =====================================================================================

SELECT '--- no common type ---';

SELECT countIf(c = 1::Int64 OR c = 2::Int64 OR c = 3::Int64) FROM t_str; -- { serverError NO_COMMON_TYPE }
SELECT countIf(c != 1::Int64 AND c != 2::Int64 AND c != 3::Int64) FROM t_str; -- { serverError NO_COMMON_TYPE }
SELECT countIf(c = 1::Int64 AND c != 1::Int64) FROM t_str; -- { serverError NO_COMMON_TYPE }
-- A FixedString expression takes the same path.
SELECT countIf(c = 1::Int64 OR c = 2::Int64 OR c = 3::Int64) FROM t_foldfs; -- { serverError NO_COMMON_TYPE }
-- ... and so does a string leaf nested below a Tuple boundary.
SELECT countIf(c = tuple(1::Int64) OR c = tuple(2::Int64) OR c = tuple(3::Int64)) FROM t_tstr; -- { serverError NO_COMMON_TYPE }

-- Two expressions, so a contradiction on the ORDINARY column reaches the global fold. Parking the
-- no-common-type term BEFORE conversion leaves it with an EMPTY converted value, which is what vetoes
-- the collapse and keeps the query's own failure visible; otherwise the answer depends on
-- `optimize_redundant_comparisons`.
DROP TABLE IF EXISTS t_strveto;
CREATE TABLE t_strveto (i Int32, c String) ENGINE = Memory;
INSERT INTO t_strveto VALUES (1, 'a'), (2, 'b');
SELECT countIf((i = 1) AND (i = 2) AND (c != 1::Int64)) FROM t_strveto; -- { serverError NO_COMMON_TYPE }
SELECT 'no-common-type term vetoes the collapse', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_strveto WHERE (i = 1) AND (i = 2) AND (c != 1::Int64)) WHERE explain ILIKE '%constant_value: UInt64_0%';
-- ... while a SAFE comparison over the same String column still folds, so the veto is scoped to the
-- no-common-type case and the fold is not disabled wholesale.
SELECT countIf((i = 1) AND (i = 2) AND (c != 'zz')) AS safe_string_result_unaffected FROM t_strveto;
SELECT 'safe string comparison still folds', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_strveto WHERE (i = 1) AND (i = 2) AND (c != 'zz')) WHERE explain ILIKE '%constant_value: UInt64_0%';
SELECT 'plain contradiction still folds (string table)', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_strveto WHERE (i = 1) AND (i = 2)) WHERE explain ILIKE '%constant_value: UInt64_0%';

-- A COMPOSITE constant against a string-family expression takes the same path and is gated by the same
-- rule: `equals` accepts it at analysis time, then `executeGeneric` raises NO_COMMON_TYPE. Only the
-- spellings whose String rendering parses back survive the round-trip guard and therefore reach the
-- rule: a multi-element `tuple(...)` renders as `(1,2)` and an `Array` as `[1]`, so both used to be
-- rewritten. Every consumer is covered here because each reaches the rule from its own call site.
SELECT countIf(c = tuple(1::Int64, 2::Int64) OR c = tuple(2::Int64, 3::Int64) OR c = tuple(3::Int64, 4::Int64)) FROM t_str; -- { serverError NO_COMMON_TYPE }
SELECT countIf(c = [1::Int64] OR c = [2::Int64] OR c = [3::Int64]) FROM t_str; -- { serverError NO_COMMON_TYPE }
SELECT countIf(c != tuple(1::Int64, 2::Int64) AND c != tuple(2::Int64, 3::Int64) AND c != tuple(3::Int64, 4::Int64)) FROM t_str; -- { serverError NO_COMMON_TYPE }
SELECT countIf(c != [1::Int64] AND c != [2::Int64] AND c != [3::Int64]) FROM t_str; -- { serverError NO_COMMON_TYPE }
SELECT countIf(c = tuple(1::Int64, 2::Int64) AND c != tuple(1::Int64, 2::Int64)) FROM t_str; -- { serverError NO_COMMON_TYPE }
SELECT countIf(c = [1::Int64] AND c != [1::Int64]) FROM t_str; -- { serverError NO_COMMON_TYPE }
-- A FixedString expression carrier takes the same path.
SELECT countIf(c = [1::Int64] AND c != [1::Int64]) FROM t_foldfs; -- { serverError NO_COMMON_TYPE }
-- The `Map` rendering (`[('a',1)]`) does not parse back, so the round-trip guard already declined it
-- before this rule existed; the row is kept so the third composite kind stays covered.
SELECT countIf(c = map('a', 1::Int64) AND c != map('a', 1::Int64)) FROM t_str; -- { serverError NO_COMMON_TYPE }
-- The global contradiction must not collapse either, on the plan and not only on the exception.
SELECT countIf((i = 1) AND (i = 2) AND (c != tuple(1::Int64, 2::Int64))) FROM t_strveto; -- { serverError NO_COMMON_TYPE }
SELECT 'composite no-common-type term vetoes the collapse', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_strveto WHERE (i = 1) AND (i = 2) AND (c != tuple(1::Int64, 2::Int64))) WHERE explain ILIKE '%constant_value: UInt64_0%';
-- The SINGLE-element `tuple(...)` spelling renders as `tuple(1)`, which does not parse back either, so
-- it was already inert. It is kept to document that the distinction is about the rendering and not
-- about the constant being composite.
SELECT countIf(c = tuple(1::Int64) OR c = tuple(2::Int64) OR c = tuple(3::Int64)) FROM t_str; -- { serverError NO_COMMON_TYPE }
SELECT countIf(c = tuple('ab')) FROM t_str; -- { serverError NO_COMMON_TYPE }

-- =====================================================================================
-- (c) Queries that the rewrite used to FAIL outright. Kept last: on an unfixed build these
--     throw, and a raw exception would abort the rest of the file.
-- =====================================================================================

SELECT '--- no longer an error ---';

SELECT countIf(c = toFixedString('ab', 5) OR c = toFixedString('cd', 5) OR c = toFixedString('ef', 5)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toFixedString('ab', 5) AS g0, c = toFixedString('cd', 5) AS g1, c = toFixedString('ef', 5) AS g2 FROM t_foldfs)) AS ground_truth FROM t_foldfs;
SELECT countIf(c != toFixedString('ab', 5) AND c != toFixedString('cd', 5) AND c != toFixedString('ef', 5)) AS res, (SELECT countIf(g0 AND g1 AND g2) FROM (SELECT c != toFixedString('ab', 5) AS g0, c != toFixedString('cd', 5) AS g1, c != toFixedString('ef', 5) AS g2 FROM t_foldfs)) AS ground_truth FROM t_foldfs;
-- The oversized string can also arrive from a plain String source, so the rule is keyed on the
-- converted value's size and not on the source type.
SELECT countIf(c = 'ab\0\0\0' OR c = 'cd\0\0\0' OR c = 'ef\0\0\0') AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'ab\0\0\0' AS g0, c = 'cd\0\0\0' AS g1, c = 'ef\0\0\0' AS g2 FROM t_foldfs)) AS ground_truth FROM t_foldfs;
SELECT countIf(c = 'ab\0\0\0\0\0' OR c = 'cd\0\0\0\0\0' OR c = 'ef\0\0\0\0\0') AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'ab\0\0\0\0\0' AS g0, c = 'cd\0\0\0\0\0' AS g1, c = 'ef\0\0\0\0\0' AS g2 FROM t_fs5)) AS ground_truth FROM t_fs5;

-- Single-alternative Variant EXPRESSIONS the rewrite used to fail on, one per mechanism. The last one
-- is the reason the decline cannot be a value or leaf check: with constants of the IDENTICAL type there
-- is no conversion at all, so every value-level test passes, and the failure comes from the Set column
-- for a Variant expression being built nullable while ColumnNullable refuses a ColumnVariant.
DROP TABLE IF EXISTS t_vfs3;
CREATE TABLE t_vfs3 (c Variant(FixedString(3))) ENGINE = Memory;
INSERT INTO t_vfs3 VALUES (toFixedString('ab', 3)), (toFixedString('cd', 3)), (toFixedString('ef', 3));
SELECT countIf(c = 'abcde' OR c = 'cdcde' OR c = 'efcde') AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = 'abcde' AS g0, c = 'cdcde' AS g1, c = 'efcde' AS g2 FROM t_vfs3)) AS ground_truth FROM t_vfs3;

DROP TABLE IF EXISTS t_vtvar;
CREATE TABLE t_vtvar (c Variant(Tuple(Variant(Float64, UInt64)))) ENGINE = Memory;
INSERT INTO t_vtvar VALUES (tuple(1.0)), (tuple(2.0)), (tuple(3.0));
SELECT countIf(c = tuple(1) OR c = tuple(2) OR c = tuple(3)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = tuple(1) AS g0, c = tuple(2) AS g1, c = tuple(3) AS g2 FROM t_vtvar)) AS ground_truth FROM t_vtvar;

DROP TABLE IF EXISTS t_vdt64;
CREATE TABLE t_vdt64 (c Variant(DateTime64(1, 'UTC'))) ENGINE = Memory;
INSERT INTO t_vdt64 VALUES (toDateTime64('1970-01-01 00:00:01.2', 1, 'UTC')), (toDateTime64('1970-01-01 00:00:02.2', 1, 'UTC')), (toDateTime64('1970-01-01 00:00:03.2', 1, 'UTC')), (toDateTime64('1970-01-01 00:00:09.9', 1, 'UTC'));
SELECT countIf(c = toDateTime64('1970-01-01 00:00:01.2', 1, 'UTC') OR c = toDateTime64('1970-01-01 00:00:02.2', 1, 'UTC') OR c = toDateTime64('1970-01-01 00:00:03.2', 1, 'UTC')) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = toDateTime64('1970-01-01 00:00:01.2', 1, 'UTC') AS g0, c = toDateTime64('1970-01-01 00:00:02.2', 1, 'UTC') AS g1, c = toDateTime64('1970-01-01 00:00:03.2', 1, 'UTC') AS g2 FROM t_vdt64)) AS ground_truth FROM t_vdt64;

-- A Variant constant whose alternative is a Tuple used to fail size validation in the set.
SELECT countIf(c = tuple('a'::Enum8('a' = 1, 'b' = 2, 'c' = 3))::Variant(Tuple(Enum8('a' = 1, 'b' = 2, 'c' = 3)), UInt64) OR c = tuple('b'::Enum8('a' = 1, 'b' = 2, 'c' = 3))::Variant(Tuple(Enum8('a' = 1, 'b' = 2, 'c' = 3)), UInt64) OR c = tuple('c'::Enum8('a' = 1, 'b' = 2, 'c' = 3))::Variant(Tuple(Enum8('a' = 1, 'b' = 2, 'c' = 3)), UInt64)) AS res, (SELECT countIf(g0 OR g1 OR g2) FROM (SELECT c = tuple('a'::Enum8('a' = 1, 'b' = 2, 'c' = 3))::Variant(Tuple(Enum8('a' = 1, 'b' = 2, 'c' = 3)), UInt64) AS g0, c = tuple('b'::Enum8('a' = 1, 'b' = 2, 'c' = 3))::Variant(Tuple(Enum8('a' = 1, 'b' = 2, 'c' = 3)), UInt64) AS g1, c = tuple('c'::Enum8('a' = 1, 'b' = 2, 'c' = 3))::Variant(Tuple(Enum8('a' = 1, 'b' = 2, 'c' = 3)), UInt64) AS g2 FROM t_tenum)) AS ground_truth FROM t_tenum;

DROP TABLE IF EXISTS t_f64;
DROP TABLE IF EXISTS t_f32;
DROP TABLE IF EXISTS t_nan;
DROP TABLE IF EXISTS t_lcf;
DROP TABLE IF EXISTS t_nf;
DROP TABLE IF EXISTS t_af;
DROP TABLE IF EXISTS t_tf;
DROP TABLE IF EXISTS t_mf;
DROP TABLE IF EXISTS t_tnan;
DROP TABLE IF EXISTS t_anan;
DROP TABLE IF EXISTS t_mnan;
DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_strfull;
DROP TABLE IF EXISTS t_lcstr;
DROP TABLE IF EXISTS t_astr;
DROP TABLE IF EXISTS t_tstr;
DROP TABLE IF EXISTS t_tenum;
DROP TABLE IF EXISTS t_estr;
DROP TABLE IF EXISTS t_efs;
DROP TABLE IF EXISTS t_var;
DROP TABLE IF EXISTS t_tvar;
DROP TABLE IF EXISTS t_date;
DROP TABLE IF EXISTS t_fs5;
DROP TABLE IF EXISTS t_enum;
DROP TABLE IF EXISTS t_dec;
DROP TABLE IF EXISTS t_u8;
DROP TABLE IF EXISTS t_uuid;
DROP TABLE IF EXISTS t_fold;
DROP TABLE IF EXISTS t_foldfs;
DROP TABLE IF EXISTS t_afnan;
DROP TABLE IF EXISTS t_mfnan;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_jveto;
DROP TABLE IF EXISTS t_strveto;
DROP TABLE IF EXISTS t_var1s;
DROP TABLE IF EXISTS t_tvar1s;
DROP TABLE IF EXISTS t_vf32;
DROP TABLE IF EXISTS t_venum;
DROP TABLE IF EXISTS t_vu256;
DROP TABLE IF EXISTS t_vfs3;
DROP TABLE IF EXISTS t_vtvar;
DROP TABLE IF EXISTS t_vdt64;
