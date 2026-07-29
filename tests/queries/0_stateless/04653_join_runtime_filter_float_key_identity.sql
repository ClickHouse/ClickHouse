-- The JOIN compares keys bitwise, so two NaNs are one key and 0.0 / -0.0 are two keys. The runtime
-- filter must agree. Its single-distinct-value fast path used `equals`/`notEquals` instead, which
-- disagrees on NaN (too strict for IN) and on signed zero (too strict for NOT IN), so matching rows
-- were dropped above the JOIN.

-- Pinned as a session SET, not per statement: a SET survives both the `old analyzer` jobs and the
-- stress `compatibility='NN.N'` randomization (applyCompatibilitySetting skips manually changed
-- settings), and the runner randomizes enable_join_runtime_filters to false 5% of the time.
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
-- Log reports no row estimate, so the probe-size cutoff cannot suppress the filter today. Pinned
-- anyway so the test keeps its filter if Log ever gains an estimate or the default changes.
SET join_runtime_filter_min_probe_rows = 0;
SET allow_suspicious_low_cardinality_types = 1;

-- Log, not MergeTree: the plan inserts no runtime filter for a MergeTree source at this size, which
-- would make every assertion below vacuous. Row `has-filter` guards that.
-- Every build side holds exactly ONE distinct value; with two or more the filter takes the Set path,
-- which was already correct.

CREATE TABLE l_f64 (k Float64) ENGINE = Log;  INSERT INTO l_f64 VALUES (nan);
CREATE TABLE r_f64 (k Float64) ENGINE = Log;  INSERT INTO r_f64 VALUES (nan);
CREATE TABLE l_f32 (k Float32) ENGINE = Log;  INSERT INTO l_f32 VALUES (nan);
CREATE TABLE r_f32 (k Float32) ENGINE = Log;  INSERT INTO r_f32 VALUES (nan);
CREATE TABLE l_bf16 (k BFloat16) ENGINE = Log; INSERT INTO l_bf16 VALUES (nan);
CREATE TABLE r_bf16 (k BFloat16) ENGINE = Log; INSERT INTO r_bf16 VALUES (nan);
CREATE TABLE l_lc (k LowCardinality(Float64)) ENGINE = Log; INSERT INTO l_lc VALUES (nan);
CREATE TABLE r_lc (k LowCardinality(Float64)) ENGINE = Log; INSERT INTO r_lc VALUES (nan);

-- 1. NaN INNER JOIN NaN: the JOIN matches the identical bit patterns, `nan = nan` does not.
SELECT 'f64 inner nan', count() FROM l_f64 JOIN r_f64 ON l_f64.k = r_f64.k SETTINGS join_algorithm = 'hash';
SELECT 'f32 inner nan', count() FROM l_f32 JOIN r_f32 ON l_f32.k = r_f32.k SETTINGS join_algorithm = 'hash';
SELECT 'bf16 inner nan', count() FROM l_bf16 JOIN r_bf16 ON l_bf16.k = r_bf16.k SETTINGS join_algorithm = 'hash';
SELECT 'lc f64 inner nan', count() FROM l_lc JOIN r_lc ON l_lc.k = r_lc.k SETTINGS join_algorithm = 'hash';
-- mixed widths: the filter target type is the Float64 supertype
SELECT 'mixed f32 f64 inner nan', count() FROM l_f32 JOIN r_f64 ON l_f32.k = r_f64.k SETTINGS join_algorithm = 'hash';

CREATE TABLE l_negzero_f64 (k Float64) ENGINE = Log;  INSERT INTO l_negzero_f64 VALUES (-0.0);
CREATE TABLE r_zero_f64 (k Float64) ENGINE = Log;     INSERT INTO r_zero_f64 VALUES (0.0);
CREATE TABLE l_negzero_f32 (k Float32) ENGINE = Log;  INSERT INTO l_negzero_f32 VALUES (-0.0);
CREATE TABLE r_zero_f32 (k Float32) ENGINE = Log;     INSERT INTO r_zero_f32 VALUES (0.0);
CREATE TABLE l_negzero_bf16 (k BFloat16) ENGINE = Log; INSERT INTO l_negzero_bf16 VALUES (-0.0);
CREATE TABLE r_zero_bf16 (k BFloat16) ENGINE = Log;    INSERT INTO r_zero_bf16 VALUES (0.0);

-- 2. -0.0 LEFT ANTI JOIN 0.0: distinct keys for the JOIN, so the row belongs in the ANTI output,
-- but `0.0 != -0.0` is false so the negated filter dropped it. No NaN involved.
SELECT 'f64 anti negzero', count() FROM l_negzero_f64 LEFT ANTI JOIN r_zero_f64 ON l_negzero_f64.k = r_zero_f64.k SETTINGS join_algorithm = 'hash';
SELECT 'f32 anti negzero', count() FROM l_negzero_f32 LEFT ANTI JOIN r_zero_f32 ON l_negzero_f32.k = r_zero_f32.k SETTINGS join_algorithm = 'hash';
SELECT 'bf16 anti negzero', count() FROM l_negzero_bf16 LEFT ANTI JOIN r_zero_bf16 ON l_negzero_bf16.k = r_zero_bf16.k SETTINGS join_algorithm = 'hash';

CREATE TABLE l_multi_negzero (k Float64, j Int64) ENGINE = Log; INSERT INTO l_multi_negzero VALUES (-0.0, 1);
CREATE TABLE r_multi_zero (k Float64, j Int64) ENGINE = Log;    INSERT INTO r_multi_zero VALUES (0.0, 1);
CREATE TABLE l_multi_nan (k Float64, j Int64) ENGINE = Log;     INSERT INTO l_multi_nan VALUES (nan, 1);
CREATE TABLE r_multi_nan (k Float64, j Int64) ENGINE = Log;     INSERT INTO r_multi_nan VALUES (nan, 1);

-- 3. Multi-key LEFT ANTI builds one filter over Tuple(Float64, Int64), so the float is nested and a
-- top-level type test would miss it.
SELECT 'tuple anti negzero', count() FROM l_multi_negzero LEFT ANTI JOIN r_multi_zero ON l_multi_negzero.k = r_multi_zero.k AND l_multi_negzero.j = r_multi_zero.j SETTINGS join_algorithm = 'hash';
-- 4. Multi-key INNER builds one filter per column.
SELECT 'multikey inner nan', count() FROM l_multi_nan JOIN r_multi_nan ON l_multi_nan.k = r_multi_nan.k AND l_multi_nan.j = r_multi_nan.j SETTINGS join_algorithm = 'hash';

CREATE TABLE l_arr_negzero (k Array(Float64)) ENGINE = Log; INSERT INTO l_arr_negzero VALUES ([-0.0]);
CREATE TABLE r_arr_zero (k Array(Float64)) ENGINE = Log;    INSERT INTO r_arr_zero VALUES ([0.0]);
CREATE TABLE l_map_negzero (k Map(String, Float64)) ENGINE = Log; INSERT INTO l_map_negzero VALUES (map('a', -0.0));
CREATE TABLE r_map_zero (k Map(String, Float64)) ENGINE = Log;    INSERT INTO r_map_zero VALUES (map('a', 0.0));
CREATE TABLE l_arrtup_negzero (k Array(Tuple(Float64, Int64))) ENGINE = Log; INSERT INTO l_arrtup_negzero VALUES ([(-0.0, 1)]);
CREATE TABLE r_arrtup_zero (k Array(Tuple(Float64, Int64))) ENGINE = Log;    INSERT INTO r_arrtup_zero VALUES ([(0.0, 1)]);

-- 5. Composite keys break only in the ANTI direction: `[0.0] = [-0.0]` is true while the JOIN keys
-- differ. Array(Tuple(...)) nests the float two levels deep, so the type walk must be transitive.
SELECT 'array anti negzero', count() FROM l_arr_negzero LEFT ANTI JOIN r_arr_zero ON l_arr_negzero.k = r_arr_zero.k SETTINGS join_algorithm = 'hash';
SELECT 'map anti negzero', count() FROM l_map_negzero LEFT ANTI JOIN r_map_zero ON l_map_negzero.k = r_map_zero.k SETTINGS join_algorithm = 'hash';
SELECT 'array tuple anti negzero', count() FROM l_arrtup_negzero LEFT ANTI JOIN r_arrtup_zero ON l_arrtup_negzero.k = r_arrtup_zero.k SETTINGS join_algorithm = 'hash';

CREATE TABLE l_arr_nan (k Array(Float64)) ENGINE = Log; INSERT INTO l_arr_nan VALUES ([nan]);
CREATE TABLE r_arr_nan (k Array(Float64)) ENGINE = Log; INSERT INTO r_arr_nan VALUES ([nan]);
CREATE TABLE l_map_nan (k Map(String, Float64)) ENGINE = Log; INSERT INTO l_map_nan VALUES (map('a', nan));
CREATE TABLE r_map_nan (k Map(String, Float64)) ENGINE = Log; INSERT INTO r_map_nan VALUES (map('a', nan));

-- Controls, correct before the fix: `equals` on a composite compares float elements bitwise, so it
-- already agreed with the JOIN on NaN. These are not regressions being fixed.
SELECT 'control array inner nan', count() FROM l_arr_nan JOIN r_arr_nan ON l_arr_nan.k = r_arr_nan.k SETTINGS join_algorithm = 'hash';
SELECT 'control map inner nan', count() FROM l_map_nan JOIN r_map_nan ON l_map_nan.k = r_map_nan.k SETTINGS join_algorithm = 'hash';

-- 6. The fast path lives in the shared filter base, so every hash algorithm was affected.
SELECT 'alg parallel_hash', count() FROM l_f64 JOIN r_f64 ON l_f64.k = r_f64.k SETTINGS join_algorithm = 'parallel_hash';
SELECT 'alg grace_hash', count() FROM l_f64 JOIN r_f64 ON l_f64.k = r_f64.k SETTINGS join_algorithm = 'grace_hash';

CREATE TABLE l_nullable (k Nullable(Float64)) ENGINE = Log; INSERT INTO l_nullable VALUES (nan);
CREATE TABLE r_nullable (k Nullable(Float64)) ENGINE = Log; INSERT INTO r_nullable VALUES (nan);
CREATE TABLE l_dec (k Decimal64(2)) ENGINE = Log; INSERT INTO l_dec VALUES (1.25);
CREATE TABLE r_dec (k Decimal64(2)) ENGINE = Log; INSERT INTO r_dec VALUES (1.25);
CREATE TABLE l_int (k Int64) ENGINE = Log; INSERT INTO l_int VALUES (7);
CREATE TABLE r_int (k Int64) ENGINE = Log; INSERT INTO r_int VALUES (7);

-- 7. Types that were already correct and must keep the fast path. Nullable was excluded by the
-- existing NULL clause; Decimal and Int have no NaN and no signed zero, so `=` is key identity.
SELECT 'control nullable inner nan', count() FROM l_nullable JOIN r_nullable ON l_nullable.k = r_nullable.k SETTINGS join_algorithm = 'hash';
SELECT 'control decimal inner', count() FROM l_dec JOIN r_dec ON l_dec.k = r_dec.k SETTINGS join_algorithm = 'hash';
SELECT 'control int inner', count() FROM l_int JOIN r_int ON l_int.k = r_int.k SETTINGS join_algorithm = 'hash';

-- 8. The other direction of each disagreement, where the filter was too permissive rather than too
-- strict. The JOIN rejects these rows itself, so they were and stay empty.
SELECT 'control negzero inner', count() FROM l_negzero_f64 JOIN r_zero_f64 ON l_negzero_f64.k = r_zero_f64.k SETTINGS join_algorithm = 'hash';
SELECT 'control nan anti', count() FROM l_f64 LEFT ANTI JOIN r_f64 ON l_f64.k = r_f64.k SETTINGS join_algorithm = 'hash';

-- 9. Anti-vacuity: everything above only tests something while the plan actually installs a runtime
-- filter for this shape. EXPLAIN PLAN checks the whole subtree, so it is insensitive to the plan
-- output format.
SELECT 'has-filter', countIf(explain ILIKE '%RuntimeFilter%') > 0
FROM (EXPLAIN PLAN SELECT count() FROM l_f64 JOIN r_f64 ON l_f64.k = r_f64.k SETTINGS join_algorithm = 'hash');
