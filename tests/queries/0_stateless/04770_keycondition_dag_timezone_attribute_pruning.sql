-- Tags: no-parallel-replicas
-- no-parallel-replicas: the `pruning_selected_parts` row below reads `ProfileEvents` from the
-- initiator's `query_log`, and with parallel replicas the counters are reported where each
-- read executes.
--
-- The key transform DAG reads the timezone from the type of the column it is handed, so a set element
-- whose `DateTime` type declares a different timezone than the key column used to be transformed under the
-- wrong timezone, naming a partition/mark the row does not live in. Every row below must equal its oracle.
--
-- The leading `SET` is load-bearing: the runner randomizes `session_timezone` and one of its draws is
-- the local zone, which would make session tz == key tz and collapse the discriminator. A query-level
-- `SET` beats the runner's client argv injection.
SET session_timezone = 'Asia/Kolkata';

DROP TABLE IF EXISTS oracle_utc;
DROP TABLE IF EXISTS k_yyyymm;
DROP TABLE IF EXISTS k_todate;
DROP TABLE IF EXISTS k_tostring;
DROP TABLE IF EXISTS k_cast;
DROP TABLE IF EXISTS k_order_g1;
DROP TABLE IF EXISTS k_order_gdef;
DROP TABLE IF EXISTS k_dt64;
DROP TABLE IF EXISTS oracle_dt64;
DROP TABLE IF EXISTS k_reverse;
DROP TABLE IF EXISTS oracle_reverse;
DROP TABLE IF EXISTS k_samename;
DROP TABLE IF EXISTS oracle_samename;
DROP TABLE IF EXISTS k_unixts;
DROP TABLE IF EXISTS k_notransform;
DROP TABLE IF EXISTS k_hour;
DROP TABLE IF EXISTS k_negation;
DROP TABLE IF EXISTS oracle_negation;
DROP TABLE IF EXISTS k_bool_neg;
DROP TABLE IF EXISTS oracle_bool_neg;
DROP TABLE IF EXISTS k_nullable;
DROP TABLE IF EXISTS oracle_nullable;
DROP TABLE IF EXISTS k_nullable_dt64;
DROP TABLE IF EXISTS oracle_nullable_dt64;
DROP TABLE IF EXISTS oracle_arr;
DROP TABLE IF EXISTS k_arr;
DROP TABLE IF EXISTS oracle_arr_dt64;
DROP TABLE IF EXISTS k_arr_dt64;
DROP TABLE IF EXISTS oracle_map;
DROP TABLE IF EXISTS k_map;
DROP TABLE IF EXISTS oracle_arr_nul;
DROP TABLE IF EXISTS k_arr_nul;
DROP TABLE IF EXISTS oracle_map_nul;
DROP TABLE IF EXISTS k_map_nul;
DROP TABLE IF EXISTS oracle_arr_tup;
DROP TABLE IF EXISTS k_arr_tup;
DROP TABLE IF EXISTS oracle_arr_arr;
DROP TABLE IF EXISTS k_arr_arr;
DROP TABLE IF EXISTS oracle_arr_lc;
DROP TABLE IF EXISTS k_arr_lc;
DROP TABLE IF EXISTS k_tup;
DROP TABLE IF EXISTS k_map_el;
DROP TABLE IF EXISTS k_arr_tup1;
DROP TABLE IF EXISTS k_map_keytype;
DROP TABLE IF EXISTS oracle_saf;
DROP TABLE IF EXISTS k_saf;
DROP TABLE IF EXISTS k_saf_arr;
DROP TABLE IF EXISTS oracle_arr_tup_bool;
DROP TABLE IF EXISTS k_arr_tup_bool;
DROP TABLE IF EXISTS oracle_bool_in;
DROP TABLE IF EXISTS k_bool_in;
DROP TABLE IF EXISTS k_hour_arr;
DROP TABLE IF EXISTS k_nul_key;
DROP TABLE IF EXISTS k_nul_key_part;
DROP TABLE IF EXISTS k_nul_key_dt64;
DROP TABLE IF EXISTS k_lc_key;
DROP TABLE IF EXISTS k_arr_nul_key;
DROP TABLE IF EXISTS k_nul_prune;
DROP TABLE IF EXISTS k_point;
DROP TABLE IF EXISTS oracle_saf_outer;
DROP TABLE IF EXISTS k_saf_outer;
DROP TABLE IF EXISTS oracle_saf_outer_bool;
DROP TABLE IF EXISTS k_saf_outer_bool;

-- The oracle: identical data and predicate, no key transform to get wrong.
CREATE TABLE oracle_utc (ts DateTime('UTC')) ENGINE = Memory;
INSERT INTO oracle_utc SELECT toDateTime(1675195200, 'UTC');
SELECT 'oracle', count() FROM (SELECT ts FROM oracle_utc WHERE ts IN (SELECT toDateTime(1675195200)));

-- Carriers 1-3: timezone-observing functions in `PARTITION BY`, key `DateTime('UTC')` vs element `DateTime`.
CREATE TABLE k_yyyymm (ts DateTime('UTC')) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY tuple();
INSERT INTO k_yyyymm SELECT toDateTime(1675195200, 'UTC');
SELECT 'partition_toYYYYMM', count() FROM (SELECT ts FROM k_yyyymm WHERE ts IN (SELECT toDateTime(1675195200)));

CREATE TABLE k_todate (ts DateTime('UTC')) ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY tuple();
INSERT INTO k_todate SELECT toDateTime(1675195200, 'UTC');
SELECT 'partition_toDate', count() FROM (SELECT ts FROM k_todate WHERE ts IN (SELECT toDateTime(1675195200)));

CREATE TABLE k_tostring (ts DateTime('UTC')) ENGINE = MergeTree PARTITION BY toString(ts) ORDER BY tuple();
INSERT INTO k_tostring SELECT toDateTime(1675195200, 'UTC');
SELECT 'partition_toString', count() FROM (SELECT ts FROM k_tostring WHERE ts IN (SELECT toDateTime(1675195200)));

-- Carrier 4: a single `CAST` key takes a separate direct-cast route inside the transform.
CREATE TABLE k_cast (ts DateTime('UTC')) ENGINE = MergeTree PARTITION BY ts::String ORDER BY tuple();
INSERT INTO k_cast SELECT toDateTime(1675195200, 'UTC');
SELECT 'partition_cast', count() FROM (SELECT ts FROM k_cast WHERE ts IN (SELECT toDateTime(1675195200)));

-- Carriers 5-6: the primary key, not the partition. `index_granularity` is randomized, so pin it in the DDL.
CREATE TABLE k_order_g1 (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY toYYYYMM(ts) SETTINGS index_granularity = 1;
INSERT INTO k_order_g1 SELECT toDateTime(1675195200, 'UTC');
SELECT 'primary_key_granularity_1', count() FROM (SELECT ts FROM k_order_g1 WHERE ts IN (SELECT toDateTime(1675195200)));

CREATE TABLE k_order_gdef (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY toYYYYMM(ts);
INSERT INTO k_order_gdef SELECT toDateTime(1675195200, 'UTC');
SELECT 'primary_key_granularity_default', count() FROM (SELECT ts FROM k_order_gdef WHERE ts IN (SELECT toDateTime(1675195200)));

-- Carrier 7: `DateTime64` equality compares the scale only, so the timezone is invisible there too.
CREATE TABLE oracle_dt64 (ts DateTime64(3, 'UTC')) ENGINE = Memory;
INSERT INTO oracle_dt64 SELECT toDateTime64(1675195200, 3, 'UTC');
SELECT 'oracle_datetime64', count() FROM (SELECT ts FROM oracle_dt64 WHERE ts IN (SELECT toDateTime64(1675195200, 3)));

CREATE TABLE k_dt64 (ts DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY tuple();
INSERT INTO k_dt64 SELECT toDateTime64(1675195200, 3, 'UTC');
SELECT 'partition_datetime64', count() FROM (SELECT ts FROM k_dt64 WHERE ts IN (SELECT toDateTime64(1675195200, 3)));

-- Carrier 8: the reverse direction. The key declares no timezone and the element declares one.
CREATE TABLE oracle_reverse (ts DateTime) ENGINE = Memory;
INSERT INTO oracle_reverse SELECT toDateTime(1675195200);
SELECT 'oracle_reverse', count() FROM (SELECT ts FROM oracle_reverse WHERE ts IN (SELECT toDateTime(1675195200, 'UTC')));

CREATE TABLE k_reverse (ts DateTime) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY tuple();
INSERT INTO k_reverse SELECT toDateTime(1675195200);
SELECT 'partition_reverse', count() FROM (SELECT ts FROM k_reverse WHERE ts IN (SELECT toDateTime(1675195200, 'UTC')));

-- Carrier 9: both sides declare a timezone explicitly, and the two differ.
SELECT 'oracle_both_explicit', count() FROM (SELECT ts FROM oracle_utc WHERE ts IN (SELECT toDateTime(1675195200, 'Asia/Kolkata')));
SELECT 'partition_both_explicit', count() FROM (SELECT ts FROM k_yyyymm WHERE ts IN (SELECT toDateTime(1675195200, 'Asia/Kolkata')));

-- Carrier 10: both types report the bare name `DateTime` yet capture different timezones, because a
-- `DateTime` type binds the session zone when it is constructed. The table must be created AND populated
-- under the first timezone; only then does the switch below leave the two captures differing.
SET session_timezone = 'UTC';
CREATE TABLE k_samename (ts DateTime) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY tuple();
INSERT INTO k_samename VALUES (1675195200);
CREATE TABLE oracle_samename (ts DateTime) ENGINE = Memory;
INSERT INTO oracle_samename VALUES (1675195200);
SET session_timezone = 'Asia/Kolkata';
SELECT 'same_name_key_type', toTypeName(ts) FROM k_samename LIMIT 1;
SELECT 'oracle_same_name', count() FROM (SELECT ts FROM oracle_samename WHERE ts IN (SELECT toDateTime(1675195200)));
SELECT 'partition_same_name', count() FROM (SELECT ts FROM k_samename WHERE ts IN (SELECT toDateTime(1675195200)));

-- Carrier 11: `has` reaches the same helper through a different route, taking the element type from the
-- array's nested type rather than from a subquery result. Disabling `optimize_rewrite_has_to_in` is
-- load-bearing: at its default the pass turns this predicate into a literal `IN`, which never reaches the
-- helper at all (measured: with the rewrite on, this reads 1 before the fix, i.e. it becomes
-- `control_literal_in`).
SET optimize_rewrite_has_to_in = 0;
SELECT 'oracle_has', count() FROM (SELECT ts FROM oracle_utc WHERE has([toDateTime(1675195200)], ts));
SELECT 'partition_has', count() FROM (SELECT ts FROM k_yyyymm WHERE has([toDateTime(1675195200)], ts));
SELECT 'control_has_element_carries_key_timezone', count() FROM (SELECT ts FROM k_yyyymm WHERE has([toDateTime(1675195200, 'UTC')], ts));
SET optimize_rewrite_has_to_in = 1;

-- Carriers 12-14: the same defect one wrapper up. `equals` on a `Nullable` delegates to the nested type, so the
-- timezone is just as invisible there, while the type id of a `Nullable` is not `DateTime`. Both settings below
-- are load-bearing: at the default `transform_null_in = 0` every row here reads 1 even before the fix, and
-- `allow_nullable_key` is a MergeTree setting, so it goes in the DDL rather than a query-level `SET`.
SET transform_null_in = 1;
CREATE TABLE oracle_nullable (ts Nullable(DateTime('UTC'))) ENGINE = Memory;
INSERT INTO oracle_nullable SELECT toDateTime(1675195200, 'UTC');
CREATE TABLE k_nullable (ts Nullable(DateTime('UTC'))) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY tuple()
    SETTINGS allow_nullable_key = 1;
INSERT INTO k_nullable SELECT toDateTime(1675195200, 'UTC');
SELECT 'oracle_nullable', count() FROM (SELECT ts FROM oracle_nullable WHERE ts IN (SELECT CAST(toDateTime(1675195200) AS Nullable(DateTime))));
SELECT 'partition_nullable', count() FROM (SELECT ts FROM k_nullable WHERE ts IN (SELECT CAST(toDateTime(1675195200) AS Nullable(DateTime))));

SET optimize_rewrite_has_to_in = 0;
SELECT 'oracle_nullable_has', count() FROM (SELECT ts FROM oracle_nullable WHERE has([CAST(toDateTime(1675195200) AS Nullable(DateTime))], ts));
SELECT 'partition_nullable_has', count() FROM (SELECT ts FROM k_nullable WHERE has([CAST(toDateTime(1675195200) AS Nullable(DateTime))], ts));
SET optimize_rewrite_has_to_in = 1;

CREATE TABLE oracle_nullable_dt64 (ts Nullable(DateTime64(3, 'UTC'))) ENGINE = Memory;
INSERT INTO oracle_nullable_dt64 SELECT toDateTime64(1675195200, 3, 'UTC');
CREATE TABLE k_nullable_dt64 (ts Nullable(DateTime64(3, 'UTC'))) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY tuple()
    SETTINGS allow_nullable_key = 1;
INSERT INTO k_nullable_dt64 SELECT toDateTime64(1675195200, 3, 'UTC');
SELECT 'oracle_nullable_dt64', count() FROM (SELECT ts FROM oracle_nullable_dt64 WHERE ts IN (SELECT CAST(toDateTime64(1675195200, 3) AS Nullable(DateTime64(3)))));
SELECT 'partition_nullable_dt64', count() FROM (SELECT ts FROM k_nullable_dt64 WHERE ts IN (SELECT CAST(toDateTime64(1675195200, 3) AS Nullable(DateTime64(3)))));

-- Two controls for the rows above, both correct before the fix. The second is what proves
-- `transform_null_in` is a real co-factor rather than decoration.
SELECT 'control_nullable_element_carries_key_timezone', count() FROM (SELECT ts FROM k_nullable WHERE ts IN (SELECT CAST(toDateTime(1675195200, 'UTC') AS Nullable(DateTime('UTC')))));
SELECT 'control_nullable_transform_null_in_off', count() FROM (SELECT ts FROM k_nullable WHERE ts IN (SELECT CAST(toDateTime(1675195200) AS Nullable(DateTime)))) SETTINGS transform_null_in = 0;
SET transform_null_in = 0;

-- Carriers 15-20: the wrapper sits on the KEY COLUMN alone. Every row above pairs a wrapped key with an
-- equally wrapped element, so the two types are `equals` and the timezone is adopted. A bare comparison
-- constant never carries the wrapper, so the pair is unequal, and the constant reached the transform in
-- its own timezone. `toYYYYMM` above is monotonic and takes a different path, which casts the constant to
-- the function's declared argument type, so the carriers below use non-monotonic key transforms.
CREATE TABLE k_nul_key (ts Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY ts::String
    SETTINGS allow_nullable_key = 1;
INSERT INTO k_nul_key SELECT toDateTime(1675195200, 'UTC');
SELECT 'order_by_nullable_key', count() FROM k_nul_key WHERE ts = toDateTime(1675195200);
SELECT 'order_by_nullable_key_in', count() FROM k_nul_key WHERE ts IN (SELECT toDateTime(1675195200));

CREATE TABLE k_nul_key_part (ts Nullable(DateTime('UTC'))) ENGINE = MergeTree PARTITION BY ts::String ORDER BY tuple()
    SETTINGS allow_nullable_key = 1;
INSERT INTO k_nul_key_part SELECT toDateTime(1675195200, 'UTC');
SELECT 'partition_by_nullable_key', count() FROM k_nul_key_part WHERE ts = toDateTime(1675195200);

CREATE TABLE k_nul_key_dt64 (ts Nullable(DateTime64(3, 'UTC'))) ENGINE = MergeTree ORDER BY ts::String
    SETTINGS allow_nullable_key = 1;
INSERT INTO k_nul_key_dt64 SELECT toDateTime64(1675195200, 3, 'UTC');
SELECT 'order_by_nullable_key_dt64', count() FROM k_nul_key_dt64 WHERE ts = toDateTime64(1675195200, 3);

-- `LowCardinality` is the other wrapper a key column can carry alone. It needs `IN` rather than `=`: a
-- scalar comparison resolves a common supertype for its two arguments first, which retypes the constant
-- onto the key column's timezone before key analysis ever sees it (measured: the `=` form of this row
-- reads 1 either way, so it would witness nothing). A set element carries its own type all the way in.
CREATE TABLE k_lc_key (ts LowCardinality(DateTime('UTC'))) ENGINE = MergeTree ORDER BY ts::String
    SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO k_lc_key SELECT toDateTime(1675195200, 'UTC');
SELECT 'order_by_lowcardinality_key_in', count() FROM k_lc_key WHERE ts IN (SELECT toDateTime(1675195200));

-- The wrapper difference one level down, so it is reached through the `Array` rather than at the top of
-- the type: only the array element is `Nullable`.
CREATE TABLE k_arr_nul_key (a Array(Nullable(DateTime('UTC')))) ENGINE = MergeTree ORDER BY a::String
    SETTINGS allow_nullable_key = 1;
INSERT INTO k_arr_nul_key SELECT [toDateTime(1675195200, 'UTC')];
SELECT 'order_by_array_nullable_key', count() FROM k_arr_nul_key WHERE a = [toDateTime(1675195200)];

-- Controls for the six rows above. The first two prove the zeros they replace were dropped rows rather
-- than an empty fixture, the next two were already correct, and the last distinguishes "the wrapper was
-- relabelled" from "the atom was declined", which would also return the right count.
SELECT 'control_nullable_key_total_rows', count() FROM k_nul_key;
SELECT 'control_nullable_key_row_matches', ts = toDateTime(1675195200) FROM k_nul_key;
SELECT 'control_nullable_key_element_carries_key_timezone', count() FROM k_nul_key WHERE ts = toDateTime(1675195200, 'UTC');
SELECT 'control_nullable_key_other_instant', count() FROM k_nul_key WHERE ts = toDateTime(1675195200 + 86400);
CREATE TABLE k_nul_prune (ts Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY ts::String
    SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO k_nul_prune SELECT toDateTime(1675195200, 'UTC') + number * 3600 FROM numbers(16);
SELECT 'control_nullable_key_pruning_used',
       countIf(explain LIKE '%Granules:%') > 0 AND countIf(explain LIKE '%Granules: 16/16%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM k_nul_prune WHERE ts = toDateTime(1675195200 + 5 * 3600));

-- A key column whose type is a custom name over a composite must keep that name through the relabel.
-- `Point` is a named `Tuple` and `wkb` selects its implementation from the type name, so handing the
-- transform a plain `Tuple` makes it throw and the atom is declined. The count stays right either way,
-- which is why this row asserts the plan: without the name it reads `Condition: true` over all granules.
CREATE TABLE k_point (p Point) ENGINE = MergeTree ORDER BY wkb(p) SETTINGS index_granularity = 1;
INSERT INTO k_point SELECT (number, number) FROM numbers(16);
SELECT 'control_custom_composite_pruning_used',
       countIf(explain LIKE '%Granules:%') > 0 AND countIf(explain LIKE '%Granules: 16/16%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM k_point WHERE p = (5., 5.)::Point);
SELECT 'control_custom_composite_count', count() FROM k_point WHERE p = (5., 5.)::Point;

-- Carriers 21-28: the same defect at arbitrary wrapper depth. `Array`, `Map`, `Tuple` and
-- `LowCardinality` all delegate `equals` to their children, so a nested timezone is exactly as
-- invisible as a bare one, while a higher-order function takes its lambda argument type from the
-- runtime wrapper type. `transform_null_in` is NOT needed here (measured: the rows below read the
-- same at either value), so it stays off.
CREATE TABLE oracle_arr (a Array(DateTime('UTC'))) ENGINE = Memory;
INSERT INTO oracle_arr SELECT [toDateTime(1675195200, 'UTC')];
CREATE TABLE k_arr (a Array(DateTime('UTC'))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> toYYYYMM(x), a)) ORDER BY tuple();
INSERT INTO k_arr SELECT [toDateTime(1675195200, 'UTC')];
SELECT 'oracle_array', count() FROM (SELECT a FROM oracle_arr WHERE a IN (SELECT [toDateTime(1675195200)]));
SELECT 'partition_array', count() FROM (SELECT a FROM k_arr WHERE a IN (SELECT [toDateTime(1675195200)]));

CREATE TABLE oracle_arr_dt64 (a Array(DateTime64(3, 'UTC'))) ENGINE = Memory;
INSERT INTO oracle_arr_dt64 SELECT [toDateTime64(1675195200, 3, 'UTC')];
CREATE TABLE k_arr_dt64 (a Array(DateTime64(3, 'UTC'))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> toYYYYMM(x), a)) ORDER BY tuple();
INSERT INTO k_arr_dt64 SELECT [toDateTime64(1675195200, 3, 'UTC')];
SELECT 'oracle_array_dt64', count() FROM (SELECT a FROM oracle_arr_dt64 WHERE a IN (SELECT [toDateTime64(1675195200, 3)]));
SELECT 'partition_array_dt64', count() FROM (SELECT a FROM k_arr_dt64 WHERE a IN (SELECT [toDateTime64(1675195200, 3)]));

CREATE TABLE oracle_map (m Map(String, DateTime('UTC'))) ENGINE = Memory;
INSERT INTO oracle_map SELECT map('x', toDateTime(1675195200, 'UTC'));
CREATE TABLE k_map (m Map(String, DateTime('UTC'))) ENGINE = MergeTree
    PARTITION BY arraySum(mapValues(mapApply((k, v) -> (k, toYYYYMM(v)), m))) ORDER BY tuple();
INSERT INTO k_map SELECT map('x', toDateTime(1675195200, 'UTC'));
SELECT 'oracle_map', count() FROM (SELECT m FROM oracle_map WHERE m IN (SELECT map('x', toDateTime(1675195200))));
SELECT 'partition_map', count() FROM (SELECT m FROM k_map WHERE m IN (SELECT map('x', toDateTime(1675195200))));

-- Depth 2: the SAME code path reaches these, which is what asserts the recursion rather than a
-- per-wrapper special case. `ifNull` keeps the key non-nullable so `arraySum` accepts it.
CREATE TABLE oracle_arr_nul (a Array(Nullable(DateTime('UTC')))) ENGINE = Memory;
INSERT INTO oracle_arr_nul SELECT [toDateTime(1675195200, 'UTC')];
CREATE TABLE k_arr_nul (a Array(Nullable(DateTime('UTC')))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> ifNull(toYYYYMM(x), 0), a)) ORDER BY tuple()
    SETTINGS allow_nullable_key = 1;
INSERT INTO k_arr_nul SELECT [toDateTime(1675195200, 'UTC')];
SELECT 'oracle_array_nullable', count() FROM (SELECT a FROM oracle_arr_nul WHERE a IN (SELECT [CAST(toDateTime(1675195200) AS Nullable(DateTime))]));
SELECT 'partition_array_nullable', count() FROM (SELECT a FROM k_arr_nul WHERE a IN (SELECT [CAST(toDateTime(1675195200) AS Nullable(DateTime))]));

CREATE TABLE oracle_map_nul (m Map(String, Nullable(DateTime('UTC')))) ENGINE = Memory;
INSERT INTO oracle_map_nul SELECT map('x', toDateTime(1675195200, 'UTC'));
CREATE TABLE k_map_nul (m Map(String, Nullable(DateTime('UTC')))) ENGINE = MergeTree
    PARTITION BY arraySum(mapValues(mapApply((k, v) -> (k, ifNull(toYYYYMM(v), 0)), m))) ORDER BY tuple()
    SETTINGS allow_nullable_key = 1;
INSERT INTO k_map_nul SELECT map('x', toDateTime(1675195200, 'UTC'));
SELECT 'oracle_map_nullable', count() FROM (SELECT m FROM oracle_map_nul WHERE m IN (SELECT map('x', CAST(toDateTime(1675195200) AS Nullable(DateTime)))));
SELECT 'partition_map_nullable', count() FROM (SELECT m FROM k_map_nul WHERE m IN (SELECT map('x', CAST(toDateTime(1675195200) AS Nullable(DateTime)))));

CREATE TABLE oracle_arr_tup (a Array(Tuple(DateTime('UTC'), UInt8))) ENGINE = Memory;
INSERT INTO oracle_arr_tup SELECT [(toDateTime(1675195200, 'UTC'), 1)];
CREATE TABLE k_arr_tup (a Array(Tuple(DateTime('UTC'), UInt8))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap((d, n) -> toYYYYMM(d) + n, a)) ORDER BY tuple();
INSERT INTO k_arr_tup SELECT [(toDateTime(1675195200, 'UTC'), 1)];
SELECT 'oracle_array_tuple', count() FROM (SELECT a FROM oracle_arr_tup WHERE a IN (SELECT [(toDateTime(1675195200), 1)]));
SELECT 'partition_array_tuple', count() FROM (SELECT a FROM k_arr_tup WHERE a IN (SELECT [(toDateTime(1675195200), 1)]));

-- A `DateTime` leaf and a CUSTOM-NAMED leaf under the same wrapper. `Bool` is `UInt8` plus a custom
-- name, and neither `DataTypeNumber::equals` nor the `Tuple`/`Array` `equals` that delegates to it can
-- see the name, so this pair is `equals`-equal while the `Bool` leaf refuses relabelling. The refusal
-- must make the whole transform DECLINE: running the DAG under the element's timezone instead is the
-- wrong prune this file exists to catch, and it reads 0 on every binary that lacks the decline.
CREATE TABLE oracle_arr_tup_bool (a Array(Tuple(DateTime('UTC'), Bool))) ENGINE = Memory;
INSERT INTO oracle_arr_tup_bool SELECT [(toDateTime(1675195200, 'UTC'), true)];
CREATE TABLE k_arr_tup_bool (a Array(Tuple(DateTime('UTC'), Bool))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap((d, b) -> toYYYYMM(d) + b, a)) ORDER BY tuple();
INSERT INTO k_arr_tup_bool SELECT [(toDateTime(1675195200, 'UTC'), true)];
SELECT 'oracle_array_tuple_bool', count() FROM (SELECT a FROM oracle_arr_tup_bool WHERE a IN (SELECT [(toDateTime(1675195200), 1::UInt8)]));
SELECT 'partition_array_tuple_bool', count() FROM (SELECT a FROM k_arr_tup_bool WHERE a IN (SELECT [(toDateTime(1675195200), 1::UInt8)]));
SELECT 'control_array_tuple_bool_element_carries_key_timezone', count() FROM (SELECT a FROM k_arr_tup_bool WHERE a IN (SELECT [(toDateTime(1675195200, 'UTC'), true)]));
SELECT 'control_array_tuple_bool_other_month', count() FROM (SELECT a FROM k_arr_tup_bool WHERE a IN (SELECT [(toDateTime(1677614400, 'UTC'), true)]));

-- The same refusal reached WITHOUT a wrapper: a bare `Bool` key, whose element arrives as `UInt8`.
-- Pre-decline the element transforms under the element's own type and names a partition that holds no
-- row. `NOT IN` further down stays correct on both arms, so this is not the negation axis.
CREATE TABLE oracle_bool_in (b Bool) ENGINE = Memory;
INSERT INTO oracle_bool_in VALUES (false), (true);
CREATE TABLE k_bool_in (b Bool) ENGINE = MergeTree PARTITION BY toString(b) ORDER BY tuple();
INSERT INTO k_bool_in VALUES (false);
INSERT INTO k_bool_in VALUES (true);
SELECT 'oracle_bool_in', count() FROM (SELECT b FROM oracle_bool_in WHERE b IN (SELECT 1::UInt8));
SELECT 'partition_bool_in', count() FROM (SELECT b FROM k_bool_in WHERE b IN (SELECT 1::UInt8));
SELECT 'partition_bool_in_false', count() FROM (SELECT b FROM k_bool_in WHERE b IN (SELECT 0::UInt8));
SELECT 'control_bool_in_absent', count() FROM (SELECT b FROM k_bool_in WHERE b IN (SELECT 2::UInt8));

CREATE TABLE oracle_arr_arr (a Array(Array(DateTime('UTC')))) ENGINE = Memory;
INSERT INTO oracle_arr_arr SELECT [[toDateTime(1675195200, 'UTC')]];
CREATE TABLE k_arr_arr (a Array(Array(DateTime('UTC')))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(y -> arraySum(arrayMap(x -> toYYYYMM(x), y)), a)) ORDER BY tuple();
INSERT INTO k_arr_arr SELECT [[toDateTime(1675195200, 'UTC')]];
SELECT 'oracle_array_array', count() FROM (SELECT a FROM oracle_arr_arr WHERE a IN (SELECT [[toDateTime(1675195200)]]));
SELECT 'partition_array_array', count() FROM (SELECT a FROM k_arr_arr WHERE a IN (SELECT [[toDateTime(1675195200)]]));

-- `LowCardinality` reaches the transform only through the CONSTANT-equality caller: building an
-- `IN` set runs `recursiveRemoveLowCardinality` over the element type, so a nested `LowCardinality`
-- never arrives there. The `IN` row further down is therefore a control, not a duplicate of this one.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE oracle_arr_lc (a Array(LowCardinality(DateTime('UTC')))) ENGINE = Memory;
INSERT INTO oracle_arr_lc SELECT [toDateTime(1675195200, 'UTC')];
CREATE TABLE k_arr_lc (a Array(LowCardinality(DateTime('UTC')))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> toYYYYMM(x), a)) ORDER BY tuple();
INSERT INTO k_arr_lc SELECT [toDateTime(1675195200, 'UTC')];
SELECT 'oracle_array_lowcardinality', count() FROM (SELECT a FROM oracle_arr_lc WHERE a = CAST([toDateTime(1675195200)] AS Array(LowCardinality(DateTime))));
SELECT 'partition_array_lowcardinality', count() FROM (SELECT a FROM k_arr_lc WHERE a = CAST([toDateTime(1675195200)] AS Array(LowCardinality(DateTime))));

-- Six controls for the wrapper rows, every one measured correct BEFORE the fix. The first four reach
-- the helper with a wrapper type but their key expression takes the leaf out before the transform, so
-- they were never carriers; the last two never reach it with a wrapped timezone at all.
CREATE TABLE k_tup (t Tuple(DateTime('UTC'))) ENGINE = MergeTree PARTITION BY toYYYYMM(t.1) ORDER BY tuple();
INSERT INTO k_tup SELECT tuple(toDateTime(1675195200, 'UTC'));
SELECT 'control_tuple_leaf_key', count() FROM (SELECT t FROM k_tup WHERE t IN (SELECT tuple(toDateTime(1675195200))));

CREATE TABLE k_map_el (m Map(String, DateTime('UTC'))) ENGINE = MergeTree PARTITION BY toYYYYMM(m['x']) ORDER BY tuple();
INSERT INTO k_map_el SELECT map('x', toDateTime(1675195200, 'UTC'));
SELECT 'control_map_element_key', count() FROM (SELECT m FROM k_map_el WHERE m IN (SELECT map('x', toDateTime(1675195200))));

CREATE TABLE k_arr_tup1 (a Array(Tuple(DateTime('UTC')))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> toYYYYMM(x.1), a)) ORDER BY tuple();
INSERT INTO k_arr_tup1 SELECT [tuple(toDateTime(1675195200, 'UTC'))];
SELECT 'control_array_tuple_leaf_key', count() FROM (SELECT a FROM k_arr_tup1 WHERE a IN (SELECT [tuple(toDateTime(1675195200))]));

CREATE TABLE k_map_keytype (m Map(DateTime('UTC'), UInt8)) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> toYYYYMM(x), mapKeys(m))) ORDER BY tuple();
INSERT INTO k_map_keytype SELECT map(toDateTime(1675195200, 'UTC'), 1);
SELECT 'control_map_key_type', count() FROM (SELECT m FROM k_map_keytype WHERE m IN (SELECT map(toDateTime(1675195200), 1)));

SELECT 'control_array_lowcardinality_in_path', count() FROM (SELECT a FROM k_arr_lc WHERE a IN (SELECT [CAST(toDateTime(1675195200) AS LowCardinality(DateTime))]));
SELECT 'control_array_element_carries_key_timezone', count() FROM (SELECT a FROM k_arr WHERE a IN (SELECT [toDateTime(1675195200, 'UTC')]));
SET allow_suspicious_low_cardinality_types = 0;

-- Carriers 29-30: a `DateTime` leaf under a CUSTOM NAME. `SimpleAggregateFunction` is a custom name
-- whose storage type is the argument type verbatim, so the timezone is invisible to `equals` here
-- too, and the name must NOT stop the relabel: the leaf is still a `DateTime`, and the value map of
-- a `DateTime` is not what the custom name changes. This is why the leaf test is checked BEFORE the
-- custom-name refusal; the reverse order declines the transform for these two rows and loses pruning.
CREATE TABLE oracle_saf (ts DateTime('UTC')) ENGINE = Memory;
INSERT INTO oracle_saf SELECT toDateTime(1675195200, 'UTC');
CREATE TABLE k_saf (ts SimpleAggregateFunction(max, DateTime('UTC'))) ENGINE = AggregatingMergeTree
    PARTITION BY toYYYYMM(ts) ORDER BY tuple();
INSERT INTO k_saf SELECT toDateTime(1675195200, 'UTC');
SELECT 'oracle_simpleaggregatefunction', count() FROM (SELECT ts FROM oracle_saf WHERE ts IN (SELECT toDateTime(1675195200)));
SELECT 'partition_simpleaggregatefunction', count() FROM (SELECT ts FROM k_saf WHERE ts IN (SELECT toDateTime(1675195200)));

CREATE TABLE k_saf_arr (a Array(SimpleAggregateFunction(max, DateTime('UTC')))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> toYYYYMM(x), a)) ORDER BY tuple();
INSERT INTO k_saf_arr SELECT [toDateTime(1675195200, 'UTC')];
SELECT 'oracle_array_simpleaggregatefunction', count() FROM (SELECT a FROM oracle_arr WHERE a IN (SELECT [toDateTime(1675195200)]));
SELECT 'partition_array_simpleaggregatefunction', count() FROM (SELECT a FROM k_saf_arr WHERE a IN (SELECT [toDateTime(1675195200)]));

-- Two controls for the rows above, both correct on master and under the fix.
SELECT 'control_simpleaggregatefunction_element_carries_key_timezone', count() FROM (SELECT ts FROM k_saf WHERE ts IN (SELECT toDateTime(1675195200, 'UTC')));
SELECT 'control_array_simpleaggregatefunction_other_month', count() FROM (SELECT a FROM k_saf_arr WHERE a IN (SELECT [toDateTime(1677614400, 'UTC')]));

-- Carrier 31: the custom name wrapped AROUND the composite that holds the moved leaf, not on the leaf.
-- `SimpleAggregateFunction` renames its argument type verbatim, so the names differ while the pair stays
-- `equals`-equal, and rebuilding the `Array` would drop the name the transform was built against.
-- Declining the atom answers the count correctly too, so the last row asserts the plan: the partition key
-- expression is printed only when the atom became a key condition, a declined one reads `Condition: true`.
CREATE TABLE oracle_saf_outer (a Array(DateTime('UTC'))) ENGINE = Memory;
INSERT INTO oracle_saf_outer SELECT [toDateTime(1675195200, 'UTC')];
INSERT INTO oracle_saf_outer SELECT [toDateTime(1677614400, 'UTC')];
CREATE TABLE k_saf_outer (a SimpleAggregateFunction(anyLast, Array(DateTime('UTC')))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> toYYYYMM(x), a)) ORDER BY tuple();
INSERT INTO k_saf_outer SELECT [toDateTime(1675195200, 'UTC')];
INSERT INTO k_saf_outer SELECT [toDateTime(1677614400, 'UTC')];
SELECT 'oracle_outer_simpleaggregatefunction', count() FROM (SELECT a FROM oracle_saf_outer WHERE a IN (SELECT [toDateTime(1675195200)]));
SELECT 'partition_outer_simpleaggregatefunction', count() FROM (SELECT a FROM k_saf_outer WHERE a IN (SELECT [toDateTime(1675195200)]));
SELECT 'control_outer_simpleaggregatefunction_other_month', count() FROM (SELECT a FROM k_saf_outer WHERE a IN (SELECT [toDateTime(1673000000)]));
SELECT 'control_outer_simpleaggregatefunction_pruning_used', countIf(explain LIKE '%arraySum%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM k_saf_outer WHERE a IN (SELECT [toDateTime(1675195200)]));

-- The leaf refusal must survive an outer custom name: `Array(UInt8)` is `equals`-equal to
-- `SimpleAggregateFunction(anyLast, Array(Bool))`, so relabelling the pair before checking its children
-- would render the element `[2]` as `[true]` and let an exact `NOT IN` / `!=` prune the row holding it.
-- Correct here and on master; both rows read 1 if the outer name is restored without that check.
CREATE TABLE oracle_saf_outer_bool (a Array(Bool)) ENGINE = Memory;
INSERT INTO oracle_saf_outer_bool SELECT [false];
INSERT INTO oracle_saf_outer_bool SELECT [true];
CREATE TABLE k_saf_outer_bool (a SimpleAggregateFunction(anyLast, Array(Bool))) ENGINE = MergeTree
    PARTITION BY toString(a) ORDER BY tuple();
INSERT INTO k_saf_outer_bool SELECT [false];
INSERT INTO k_saf_outer_bool SELECT [true];
SELECT 'oracle_outer_bool_negation', count() FROM (SELECT a FROM oracle_saf_outer_bool WHERE a NOT IN (SELECT [2::UInt8]));
SELECT 'control_outer_bool_negation', count() FROM (SELECT a FROM k_saf_outer_bool WHERE a NOT IN (SELECT [2::UInt8]));
SELECT 'control_outer_bool_neq', count() FROM (SELECT a FROM k_saf_outer_bool WHERE a != [2::UInt8]);

-- Controls. Each was measured correct before the fix, so they are what proves the carriers above
-- discriminate rather than the whole file simply reading 1.
CREATE TABLE k_unixts (ts DateTime('UTC')) ENGINE = MergeTree PARTITION BY toUnixTimestamp(ts) ORDER BY tuple();
INSERT INTO k_unixts SELECT toDateTime(1675195200, 'UTC');
SELECT 'control_timezone_blind_dag', count() FROM (SELECT ts FROM k_unixts WHERE ts IN (SELECT toDateTime(1675195200)));

CREATE TABLE k_notransform (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY ts;
INSERT INTO k_notransform SELECT toDateTime(1675195200, 'UTC');
SELECT 'control_no_key_transform', count() FROM (SELECT ts FROM k_notransform WHERE ts IN (SELECT toDateTime(1675195200)));

SELECT 'control_element_carries_key_timezone', count() FROM (SELECT ts FROM k_yyyymm WHERE ts IN (SELECT toDateTime(1675195200, 'UTC')));
SELECT 'control_scalar_equality', count() FROM (SELECT ts FROM k_yyyymm WHERE ts = toDateTime(1675195200));
SELECT 'control_literal_in', count() FROM (SELECT ts FROM k_yyyymm WHERE ts IN (toDateTime(1675195200)));
SELECT 'control_range', count() FROM (SELECT ts FROM k_yyyymm WHERE ts >= toDateTime(1675195200) AND ts <= toDateTime(1675195200));

-- Control: with the session timezone equal to the key's, the two types agree and there is nothing to relabel.
SET session_timezone = 'UTC';
SELECT 'control_session_timezone_equals_key', count() FROM (SELECT ts FROM k_yyyymm WHERE ts IN (SELECT toDateTime(1675195200)));
SET session_timezone = 'Asia/Kolkata';

-- Controls, not carriers: relabeling a `DateTime` type is a one-to-one change of which value the transform
-- produces, so it cannot make an exact `NOT IN` / `!=` drop a matching row. Both must equal the oracle before
-- and after the fix. Two partitions and an element naming one of them, so a wrong prune would be visible.
CREATE TABLE k_negation (ts DateTime('UTC')) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY tuple();
INSERT INTO k_negation SELECT toDateTime(1675195200, 'UTC');
INSERT INTO k_negation SELECT toDateTime(1677614400, 'UTC');
CREATE TABLE oracle_negation (ts DateTime('UTC')) ENGINE = Memory;
INSERT INTO oracle_negation SELECT toDateTime(1675195200, 'UTC');
INSERT INTO oracle_negation SELECT toDateTime(1677614400, 'UTC');
SELECT 'oracle_negation_not_in', arraySort(groupArray(toUnixTimestamp(ts))) FROM (SELECT ts FROM oracle_negation WHERE ts NOT IN (SELECT toDateTime(1675195200)));
SELECT 'control_negation_not_in', arraySort(groupArray(toUnixTimestamp(ts))) FROM (SELECT ts FROM k_negation WHERE ts NOT IN (SELECT toDateTime(1675195200)));
SELECT 'oracle_negation_neq', arraySort(groupArray(toUnixTimestamp(ts))) FROM (SELECT ts FROM oracle_negation WHERE ts != toDateTime(1675195200));
SELECT 'control_negation_neq', arraySort(groupArray(toUnixTimestamp(ts))) FROM (SELECT ts FROM k_negation WHERE ts != toDateTime(1675195200));

-- The relabel above is restricted to `DateTime`/`DateTime64` for a reason, and this row is what pins it.
-- `Bool` is `UInt8` plus a custom name, equally invisible to `equals`, but its serialization renders every
-- nonzero value as `true`, so relabeling a bare `UInt8` element to `Bool` would collapse the transform's
-- value map. `toString` reports itself injective, nothing marks the condition relaxed, and an exact
-- `NOT IN` then prunes
-- the partition holding the matching row. Correct here and on master; reads 1 if the restriction is dropped.
CREATE TABLE k_bool_neg (b Bool) ENGINE = MergeTree PARTITION BY toString(b) ORDER BY tuple();
INSERT INTO k_bool_neg VALUES (false);
INSERT INTO k_bool_neg VALUES (true);
CREATE TABLE oracle_bool_neg (b Bool) ENGINE = Memory;
INSERT INTO oracle_bool_neg VALUES (false), (true);
SELECT 'oracle_bool_negation', count() FROM (SELECT b FROM oracle_bool_neg WHERE b NOT IN (SELECT 2::UInt8));
SELECT 'control_bool_negation', count() FROM (SELECT b FROM k_bool_neg WHERE b NOT IN (SELECT 2::UInt8));

-- The rows above assert answers, so they also pass if the transform is DECLINED rather than corrected: the
-- key condition is then simply not built, every part is scanned and `count` is still right. The rows below
-- assert that pruning is USED, which is the property the fix was chosen for over declining.
--
-- Two properties of this fixture are load-bearing. Two partitions, because with one part pruning-used and
-- pruning-declined both select it. And the two hours interleaved across two days, so the element's instant
-- lies inside the ts range of BOTH parts: the min-max index then keeps 2/2 and cannot stand in for the
-- partition index, which is what leaves `SelectedParts` free to move (measured: with the parts separated in
-- time, min-max prunes 1/2 on its own and this reads 1 whether the partition index ran or not).
--
-- `SelectedParts` is counted where each read executes, so with parallel replicas it is no longer a
-- function of whether the partition index ran on the initiator. The tag covers the CI flavour that
-- enables them in the default profile; this pins any other path that reaches the test with them on.
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
CREATE TABLE k_hour (ts DateTime('UTC')) ENGINE = MergeTree PARTITION BY toHour(ts) ORDER BY tuple();
INSERT INTO k_hour SELECT toDateTime(1675267200, 'UTC') UNION ALL SELECT toDateTime(1675285200, 'UTC')
                UNION ALL SELECT toDateTime(1675440000, 'UTC') UNION ALL SELECT toDateTime(1675458000, 'UTC');
OPTIMIZE TABLE k_hour FINAL;
SELECT 'pruning_active_parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'k_hour' AND active;
SELECT 'pruning_count', count() FROM (SELECT ts FROM k_hour WHERE ts IN (SELECT toDateTime(1675440000))) SETTINGS log_comment = '04770_pruning_oracle';
SYSTEM FLUSH LOGS query_log;
SELECT 'pruning_selected_parts', ProfileEvents['SelectedParts'] FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = '04770_pruning_oracle';

-- The same pruning-USE assertion one wrapper down. The block above is a BARE `DateTime` key, so it
-- pins no recursive branch: every wrapper carrier asserts only `count`, and a declined transform
-- yields a full scan whose `count` is still right. This row is what distinguishes "the wrapper was
-- relabelled" from "the wrapper was declined". Same two load-bearing fixture properties as above: two
-- parts, and the element inside the `a` range of both, so min-max keeps 2/2 and cannot stand in.
CREATE TABLE k_hour_arr (a Array(DateTime('UTC'))) ENGINE = MergeTree
    PARTITION BY arraySum(arrayMap(x -> toHour(x), a)) ORDER BY tuple();
INSERT INTO k_hour_arr SELECT [toDateTime(1675267200, 'UTC')] UNION ALL SELECT [toDateTime(1675285200, 'UTC')]
                UNION ALL SELECT [toDateTime(1675440000, 'UTC')] UNION ALL SELECT [toDateTime(1675458000, 'UTC')];
OPTIMIZE TABLE k_hour_arr FINAL;
SELECT 'pruning_arr_active_parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'k_hour_arr' AND active;
SELECT 'pruning_arr_count', count() FROM (SELECT a FROM k_hour_arr WHERE a IN (SELECT [toDateTime(1675440000)])) SETTINGS log_comment = '04770_pruning_arr';
SYSTEM FLUSH LOGS query_log;
SELECT 'pruning_arr_selected_parts', ProfileEvents['SelectedParts'] FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = '04770_pruning_arr';

DROP TABLE oracle_utc;
DROP TABLE k_yyyymm;
DROP TABLE k_todate;
DROP TABLE k_tostring;
DROP TABLE k_cast;
DROP TABLE k_order_g1;
DROP TABLE k_order_gdef;
DROP TABLE k_dt64;
DROP TABLE oracle_dt64;
DROP TABLE k_reverse;
DROP TABLE oracle_reverse;
DROP TABLE k_samename;
DROP TABLE oracle_samename;
DROP TABLE k_unixts;
DROP TABLE k_notransform;
DROP TABLE k_hour;
DROP TABLE k_negation;
DROP TABLE oracle_negation;
DROP TABLE k_bool_neg;
DROP TABLE oracle_bool_neg;
DROP TABLE k_nullable;
DROP TABLE oracle_nullable;
DROP TABLE k_nullable_dt64;
DROP TABLE oracle_nullable_dt64;
DROP TABLE oracle_arr;
DROP TABLE k_arr;
DROP TABLE oracle_arr_dt64;
DROP TABLE k_arr_dt64;
DROP TABLE oracle_map;
DROP TABLE k_map;
DROP TABLE oracle_arr_nul;
DROP TABLE k_arr_nul;
DROP TABLE oracle_map_nul;
DROP TABLE k_map_nul;
DROP TABLE oracle_arr_tup;
DROP TABLE k_arr_tup;
DROP TABLE oracle_arr_arr;
DROP TABLE k_arr_arr;
DROP TABLE oracle_arr_lc;
DROP TABLE k_arr_lc;
DROP TABLE k_tup;
DROP TABLE k_map_el;
DROP TABLE k_arr_tup1;
DROP TABLE oracle_arr_tup_bool;
DROP TABLE k_arr_tup_bool;
DROP TABLE oracle_bool_in;
DROP TABLE k_bool_in;
DROP TABLE k_hour_arr;
DROP TABLE k_map_keytype;
DROP TABLE oracle_saf;
DROP TABLE k_saf;
DROP TABLE k_saf_arr;
DROP TABLE k_nul_key;
DROP TABLE k_nul_key_part;
DROP TABLE k_nul_key_dt64;
DROP TABLE k_lc_key;
DROP TABLE k_arr_nul_key;
DROP TABLE k_nul_prune;
DROP TABLE k_point;
DROP TABLE oracle_saf_outer;
DROP TABLE k_saf_outer;
DROP TABLE oracle_saf_outer_bool;
DROP TABLE k_saf_outer_bool;
