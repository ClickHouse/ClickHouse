-- An Enum constant compared against a String/FixedString column used to be converted to the enum's
-- underlying number instead of its name, so key analysis, skip indexes and IN sets all used the wrong
-- bytes. Every assertion below prints 1.
--
-- The enum is Enum8('7' = 3): the name '7' and the number 3 are both valid strings and both are stored,
-- so a cell distinguishes "returned nothing" from "returned the WRONG row".
--
-- Reference values: for = and range predicates the reference is an unindexed table, which is correct
-- even before the fix. For the IN family the reference is the equivalent String literal, because IN set
-- construction goes through convertFieldToType and is storage independent, so an unindexed table returns
-- the same wrong answer and could not detect the bug.

-- Half the assertions read an index section out of EXPLAIN indexes = 1, and a remote-only parallel
-- replicas plan is a single ReadFromRemoteParallelReplicas node carrying no index section, so every one of
-- them would read 0. The runner randomizes parallel_replicas_local_plan, so parallel replicas are turned
-- off here for the session rather than tagging the test out of that job entirely: the conversion under
-- test is not parallel replicas specific, so the test should still run there.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ref_str;
DROP TABLE IF EXISTS pk_str;
DROP TABLE IF EXISTS pk_lc;
DROP TABLE IF EXISTS pk_nullable;
DROP TABLE IF EXISTS pk_fixed1;
DROP TABLE IF EXISTS pk_fixed4;
DROP TABLE IF EXISTS pk_partition;
DROP TABLE IF EXISTS bf_str;
DROP TABLE IF EXISTS bf_fixed4;
DROP TABLE IF EXISTS bf_array;
DROP TABLE IF EXISTS pk_pair;
DROP TABLE IF EXISTS pk_fixed10;
DROP TABLE IF EXISTS cast_ref;

CREATE TABLE ref_str (v String) ENGINE = Log;
INSERT INTO ref_str VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE pk_str (v String) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO pk_str VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE pk_lc (v LowCardinality(String)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO pk_lc VALUES ('7'), ('3'), ('V0'), ('zz');

CREATE TABLE pk_nullable (v Nullable(String)) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO pk_nullable VALUES ('7'), ('3'), ('V0'), ('zz');

-- FixedString(1) is exactly as wide as the one byte name, FixedString(4) is wider: the wider one only
-- matches if the converted name is zero padded to the column width.
CREATE TABLE pk_fixed1 (v FixedString(1)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO pk_fixed1 VALUES ('7'), ('3');

-- Eight rows, so that with index_granularity = 1 a point lookup leaves granules unread and the pruning
-- oracle below can require selected < total. With only two rows both arms read everything and the
-- non vacuity check could not fail.
CREATE TABLE pk_fixed4 (v FixedString(4)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO pk_fixed4 VALUES ('7'), ('3'), ('V0'), ('zz'), ('aa'), ('bb'), ('cc'), ('dd');

-- The '3' partition deliberately holds more rows than the '7' partition. The wrongly converted constant
-- selects the '3' partition, so its granule count differs from the correct one and the partition pruning
-- oracle below is a real detector rather than a coincidence.
CREATE TABLE pk_partition (v String) ENGINE = MergeTree PARTITION BY v ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO pk_partition VALUES ('7'), ('3'), ('3'), ('3'), ('V0');

-- The bloom filter tables deliberately do NOT store the string '3', and they use a low false positive
-- rate. With the default rate this fixture is small enough that '3' collides with an existing granule,
-- so the wrongly converted constant would keep the right granule by accident and the cell would pass
-- before the fix. Measured on the default rate: v = '3' yields Granules: 1/4 although no row matches.
CREATE TABLE bf_str (id UInt64, v String, INDEX idx v TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_str VALUES (0, '7'), (1, 'V0'), (2, 'zz'), (3, 'qq');

CREATE TABLE bf_fixed4 (id UInt64, v FixedString(4), INDEX idx v TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_fixed4 VALUES (0, '7'), (1, 'V0'), (2, 'zz'), (3, 'qq');

CREATE TABLE bf_array (id UInt64, v Array(String), INDEX idx v TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_array VALUES (0, ['7']), (1, ['V0']);

CREATE TABLE pk_pair (a String, b String) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1;
INSERT INTO pk_pair VALUES ('7', 'x'), ('3', 'x'), ('V0', 'x');

-- Pins that the FixedString padding path itself is unchanged (the 01503_fixed_string_primary_key shape).
CREATE TABLE pk_fixed10 (key FixedString(10), i Int) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO pk_fixed10 SELECT toFixedString(toString(number % 10), 10), number FROM numbers(80);

-- INSERT ... SELECT converts through castColumn, which was already correct: the reference for values().
CREATE TABLE cast_ref (x String) ENGINE = Log;
INSERT INTO cast_ref SELECT CAST('7', 'Enum8(\'7\' = 3)');

SELECT 'pk_equals', (SELECT groupArray(v) FROM pk_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_less', (SELECT groupArray(v) FROM pk_str WHERE v < CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v < CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_not_equals_control', (SELECT arraySort(groupArray(v)) FROM pk_str WHERE v != CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT arraySort(groupArray(v)) FROM ref_str WHERE v != CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_low_cardinality', (SELECT groupArray(toString(v)) FROM pk_lc WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_nullable', (SELECT groupArray(v) FROM pk_nullable WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'pk_fixed_string_narrow', (SELECT groupArray(toString(v)) FROM pk_fixed1 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0) = ['7'];

SELECT 'pk_fixed_string_wide', (SELECT groupArray(trim(toString(v))) FROM pk_fixed4 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0) = ['7'];

SELECT 'partition_key', (SELECT groupArray(v) FROM pk_partition WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

SELECT 'bloom_filter_equals', (SELECT groupArray(v) FROM bf_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'))
    = (SELECT groupArray(v) FROM bf_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)') SETTINGS use_skip_indexes = 0);

-- Only matches if the name is padded to the column width before it is hashed.
SELECT 'bloom_filter_fixed_string', (SELECT groupArray(trim(toString(v))) FROM bf_fixed4 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')) = ['7'];

SELECT 'bloom_filter_in', (SELECT groupArray(v) FROM bf_str WHERE v IN (CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT groupArray(v) FROM bf_str WHERE v IN ('7'));

SELECT 'bloom_filter_has', (SELECT groupArray(v) FROM bf_array WHERE has(v, CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT groupArray(v) FROM bf_array WHERE has(v, CAST('7', 'Enum8(\'7\' = 3)')) SETTINGS use_skip_indexes = 0);

SELECT 'in_set', (SELECT groupArray(v) FROM ref_str WHERE v IN (CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT groupArray(v) FROM ref_str WHERE v IN ('7'));

-- NOT IN was inverted, not merely over pruning: it excluded the number and returned the name.
SELECT 'not_in', (SELECT arraySort(groupArray(v)) FROM pk_str WHERE v NOT IN (CAST('7', 'Enum8(\'7\' = 3)'))
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT arraySort(groupArray(v)) FROM pk_str WHERE v NOT IN ('7')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0);

-- An OR chain of enum constants is rewritten to IN, which used to drop a disjunct. Three disjuncts are
-- needed: optimize_min_equality_disjunction_chain_length defaults to 3, so a two disjunct chain is left
-- as OR and would only measure two standalone equals, which pk_equals already covers. The cell after
-- this one asserts the rewrite really happens, so the chain length cannot silently fall below it again.
SELECT 'or_chain_rewritten_to_in', (SELECT arraySort(groupArray(v)) FROM pk_str
    WHERE v = CAST('7', 'Enum8(\'7\' = 3, \'zz\' = 9, \'V0\' = 1)')
       OR v = CAST('zz', 'Enum8(\'7\' = 3, \'zz\' = 9, \'V0\' = 1)')
       OR v = CAST('V0', 'Enum8(\'7\' = 3, \'zz\' = 9, \'V0\' = 1)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT arraySort(groupArray(v)) FROM pk_str WHERE v IN ('7', 'zz', 'V0')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0);

-- Guards the chain length that the cell above depends on: three disjuncts must still collapse into a
-- single IN at the default optimize_min_equality_disjunction_chain_length, so or_chain_rewritten_to_in
-- cannot silently degrade into two standalone equals that pk_equals already covers. The disjuncts are
-- deliberately plain String literals, not enum constants, so this cell measures the chain length
-- mechanism only and stays independent of whether an enum constant is eligible for the rewrite at all,
-- which is a separate question that index and rewrite eligibility rules may legitimately change.
-- Matched with the trailing comma, because a bare '%in%' also matches ordinary, result_type and other
-- identifiers. This is a mechanism guard rather than a bug detector: the rewrite fires before the fix too.
-- enable_analyzer is pinned because EXPLAIN QUERY TREE throws NOT_IMPLEMENTED without the analyzer, which
-- would abort the whole file on the old analyzer job and under compatibility settings below 24.3. The pin
-- belongs on the outer SELECT: inside the subquery it is rejected as a setting changed in a subquery.
SELECT 'or_chain_is_rewritten', countIf(explain LIKE '%function_name: in,%') = 1
    AND countIf(explain LIKE '%function_name: or,%') = 0
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM pk_str
    WHERE v = '7' OR v = 'zz' OR v = 'V0')
SETTINGS enable_analyzer = 1;

SELECT 'tuple_in', (SELECT groupArray(a) FROM pk_pair WHERE (a, b) IN ((CAST('7', 'Enum8(\'7\' = 3)'), 'x'))
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(a) FROM pk_pair WHERE (a, b) IN (('7', 'x'))
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0);

-- Nullable(Enum) reaches the conversion with the wrapper still on the source type hint.
SELECT 'nullable_enum_equals', (SELECT groupArray(v) FROM pk_str
    WHERE v = CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Nullable(Enum8('7' = 3)))
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = '7');

SELECT 'nullable_enum_in', (SELECT groupArray(v) FROM ref_str
    WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Nullable(Enum8('7' = 3)))))
    = (SELECT groupArray(v) FROM ref_str WHERE v IN ('7'));

SELECT 'nullable_enum_values', (SELECT hex(x) FROM values('x String', CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Nullable(Enum8('7' = 3)))))
    = (SELECT hex(x) FROM cast_ref);

-- values() used to write the number where INSERT ... SELECT writes the name.
SELECT 'values_table_function', (SELECT hex(x) FROM values('x String', CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT hex(x) FROM cast_ref);

SELECT 'enum_with_extra_label', (SELECT groupArray(v) FROM pk_str WHERE v = CAST('7', 'Enum8(\'7\' = 3, \'nope\' = 9)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = '7');

SELECT 'enum16', (SELECT groupArray(v) FROM pk_str WHERE v = CAST('7', 'Enum16(\'7\' = 3)')
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)
    = (SELECT groupArray(v) FROM ref_str WHERE v = '7');

-- Correctness could also be restored by declining the index, which would silently cost pruning, so the
-- enum constant must prune exactly as much as the equivalent String literal.
SELECT 'pk_still_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_str WHERE v = '7'
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

-- The reference above would also be satisfied if the enum constant made key analysis give up, because a
-- declined index reads every granule. This pins that some granules really are skipped, so the constant is
-- still turned into a usable range rather than being dropped.
SELECT 'pk_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

SELECT 'bloom_filter_still_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_str WHERE v = '7'));

-- The same non vacuity argument as pk_prunes_something, once per remaining surface: comparing the enum
-- arm's granule total against the String literal arm's is also satisfied when the index DECLINES for both
-- operands, because two full scans are equal. Each companion below pins that granules really are skipped.
-- These six are GUARDS, not detectors: they print 1 on unfixed master too, because there the constant
-- still narrows the read, just to the wrong granule. A permanently green cell here is expected, not
-- vacuous; partition_key_condition_uses_name below is the cell that detects the substitution itself.
SELECT 'bloom_filter_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)')));

-- One pruning oracle per distinct conversion branch, because the cells above cover only the plain String
-- primary key and the plain String bloom filter. Without these, a regression that restored correctness by
-- making the branch decline the index would still return the right rows and every result cell would pass.
-- Each compares the enum constant's granule total against the equivalent String literal's.

-- FixedString re-entry: the name has to be zero padded to the column width by the second pass.
SELECT 'pk_fixed_string_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_fixed4 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_fixed4 WHERE v = '7'
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

-- Non vacuous companion: a declined index reads every granule, which would satisfy the equality above
-- with both arms reading everything. This pins that granules really are skipped.
SELECT 'pk_fixed_string_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_fixed4 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

-- PARTITION BY: this plan emits both Min-Max and Partition sections, and both carry a Granules: line, so
-- the same total over all sections is used here as everywhere else.
-- The three cells that read a plan on this surface pin optimize_trivial_count_query off. Without the pin the
-- old analyzer answers count() from the partition predicate alone, which the analyzer declines to do, so
-- the plan collapses to ReadFromPreparedSource and prints no Indexes section at all: the granule cell then
-- compares 0 against 0 and passes vacuously, and the condition cell below reads 0 for lack of any text to
-- match. The count cell further down deliberately leaves the optimization enabled, because answering
-- count() from the partition predicate is itself a route that consumed the converted constant.
SELECT 'partition_key_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_partition WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_partition WHERE v = '7'
          SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0));

-- The equality above is satisfied by two full scans, and it measures nothing at all if the pinned plan
-- carries no granule counts, which is what happens with the pin removed, so assert the narrowing
-- separately. Every stage's denominator is the previous stage's numerator, so summing over sections lets
-- one stage's pruning stand in for another's. The two cells below therefore add use_skip_indexes = 0,
-- which leaves Min-Max inert at 5/5 with condition true and makes Partition the stage that prunes, and
-- they read that section by name: measured 1/5 for Partition where the default plan reports 1/1.
SELECT 'partition_key_prunes_something', (SELECT
    toUInt64OrZero(extract(arrayStringConcat(groupArray(explain), '|'), 'Partition.*?Granules: (\\d+)/'))
      < toUInt64OrZero(extract(arrayStringConcat(groupArray(explain), '|'), 'Partition.*?Granules: \\d+/(\\d+)'))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_partition WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0,
                   use_skip_indexes = 0));

-- The condition text is strictly stronger than a granule count, because it names which value the key
-- analysis used and so detects the name versus number substitution directly. It is read out of the
-- Partition section alone, so pruning by another stage cannot satisfy it.
SELECT 'partition_key_condition_uses_name', (SELECT
    extract(arrayStringConcat(groupArray(explain), '|'), 'Partition.*?Condition: \\(([^)]*)\\)')
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_partition WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0,
                   use_skip_indexes = 0)) = 'v in [\'7\', \'7\']';

-- The three cells above pin optimize_trivial_count_query off in order to read a plan, which leaves the route
-- where count() is answered from the partition predicate alone unmeasured. That route is a separate
-- consumer of the converted constant and it fails in the other direction: instead of dropping the matching
-- row it counted the rows of the '3' partition, returning 3 for a true count of 1. This cell pins the
-- setting ON rather than off, both to select that route and because the test runner otherwise disables the
-- optimization at random in about one run in twenty. Whether the route is then taken depends on the
-- analyzer, which cannot be pinned inside a subquery, so the cell asserts the count itself: it reads the
-- correct value on either analyzer after the fix and the wrong one on both before it.
SELECT 'partition_key_count', (SELECT count() FROM pk_partition WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
    SETTINGS optimize_trivial_count_query = 1, optimize_use_implicit_projections = 0)
    = (SELECT count() FROM ref_str WHERE v = CAST('7', 'Enum8(\'7\' = 3)'));

-- The cell above only covers that route while the route is actually selected, and only the old analyzer
-- selects it: the planner declines trivial count whenever a WHERE is present. Rather than pin an analyzer,
-- which a subquery cannot do, assert that route selection still follows the analyzer in use. This reads 1
-- on either analyzer today and turns 0 if the old analyzer stops taking the partition predicate route,
-- which is the case that would quietly reduce the cell above to an ordinary scan.
SELECT 'partition_key_count_route_follows_analyzer', (SELECT countIf(explain LIKE '%Optimized trivial count%') > 0
    FROM (EXPLAIN SELECT count() FROM pk_partition WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS optimize_trivial_count_query = 1, optimize_use_implicit_projections = 0))
    = (SELECT NOT getSetting('enable_analyzer'));

SELECT 'bloom_filter_fixed_string_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_fixed4 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_fixed4 WHERE v = '7'));

SELECT 'bloom_filter_fixed_string_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_fixed4 WHERE v = CAST('7', 'Enum8(\'7\' = 3)')));

-- The IN set branch, distinct from the equals branch pinned by bloom_filter_still_prunes.
SELECT 'bloom_filter_in_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_str WHERE v IN (CAST('7', 'Enum8(\'7\' = 3)'))))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_str WHERE v IN ('7')));

SELECT 'bloom_filter_in_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_str WHERE v IN (CAST('7', 'Enum8(\'7\' = 3)'))));

-- The array element with hint branch.
SELECT 'bloom_filter_has_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_array WHERE has(v, CAST('7', 'Enum8(\'7\' = 3)'))))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_array WHERE has(v, '7')));

SELECT 'bloom_filter_has_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_array WHERE has(v, CAST('7', 'Enum8(\'7\' = 3)'))));

-- A LowCardinality or Nullable wrapper on the key type. The source hint unwrap branch is covered by the
-- Nullable(Enum) cells further up; here the wrapper is on the target.
SELECT 'pk_low_cardinality_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_lc WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_lc WHERE v = '7'
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

SELECT 'pk_low_cardinality_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_lc WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

SELECT 'pk_nullable_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_nullable WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_nullable WHERE v = '7'
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

SELECT 'pk_nullable_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_nullable WHERE v = CAST('7', 'Enum8(\'7\' = 3)')
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

-- The tuple set branch does carry a distinct pruning signal, measured as 2/3 granules against 1/3 before
-- the fix, so it gets an oracle like the others rather than a documented gap.
SELECT 'tuple_in_prunes', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_pair WHERE (a, b) IN ((CAST('7', 'Enum8(\'7\' = 3)'), 'x'))
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0))
    = (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_pair WHERE (a, b) IN (('7', 'x'))
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

SELECT 'tuple_in_prunes_something', (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')))
    < sum(toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')))
    FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_pair WHERE (a, b) IN ((CAST('7', 'Enum8(\'7\' = 3)'), 'x'))
          SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0));

-- Conversions that must not change.
SELECT 'control_string_to_enum', toInt8(CAST('7', 'Enum8(\'7\' = 3)')) = 3;
SELECT 'control_enum_to_string_cast', CAST(CAST('7', 'Enum8(\'7\' = 3)') AS String) = '7';
SELECT 'control_bool_to_string', CAST(true, 'String') = 'true';
SELECT 'control_plain_string_constant', (SELECT groupArray(v) FROM pk_str WHERE v = '7'
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0) = ['7'];
SELECT 'control_fixed_string_padding', (SELECT count() FROM pk_fixed10 WHERE key = '1'
    SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0) = 8;

-- An Enum element inside a container is still converted to the number, because the Array/Tuple/Map
-- recursion of convertFieldToType passes no element type hint down, and so does
-- createColumnFromConstantArray in the bloom filter condition. The cells below assert the current
-- (wrong) values rather than the desired ones, so the limitation is documented instead of hidden.
-- The hint propagation is added by https://github.com/ClickHouse/ClickHouse/pull/110084.
SELECT 'known_limitation_array_element', (SELECT hex(x[1]) FROM values('x Array(String)', [CAST('7', 'Enum8(\'7\' = 3)')])) = '33';

-- hasAny and hasAll go through createColumnFromConstantArray, so their bloom filter lookup still uses
-- the number and over prunes. Unlike has, which converts the element with the hint and is fixed above.
SELECT 'known_limitation_has_any', (SELECT groupArray(v) FROM bf_array WHERE hasAny(v, [CAST('7', 'Enum8(\'7\' = 3)')])) = [];
SELECT 'known_limitation_has_all', (SELECT groupArray(v) FROM bf_array WHERE hasAll(v, [CAST('7', 'Enum8(\'7\' = 3)')])) = [];

DROP TABLE ref_str;
DROP TABLE pk_str;
DROP TABLE pk_lc;
DROP TABLE pk_nullable;
DROP TABLE pk_fixed1;
DROP TABLE pk_fixed4;
DROP TABLE pk_partition;
DROP TABLE bf_str;
DROP TABLE bf_fixed4;
DROP TABLE bf_array;
DROP TABLE pk_pair;
DROP TABLE pk_fixed10;
DROP TABLE cast_ref;
