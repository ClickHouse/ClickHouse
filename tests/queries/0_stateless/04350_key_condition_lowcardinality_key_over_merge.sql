-- Regression test for a LOGICAL_ERROR ("Arguments of 'minus'/'plus' have incorrect data types")
-- during primary-key index analysis when a monotonic arithmetic function is applied over a
-- LowCardinality key column exposed through a Merge table whose header declares the plain type.
-- KeyCondition::applyFunction executed the monotonic function on the raw LowCardinality index
-- column while the function was resolved against the plain key type, throwing a LOGICAL_ERROR
-- exception. The function must be applied to the full (non-LowCardinality) representation, as the
-- sibling applyFunctionChainToColumn already does.

SET allow_suspicious_low_cardinality_types = 1;
-- The sparse (lightweight) primary-key analysis overload is selected by this setting, which the
-- test runner randomizes; pin it so the sparse cases below always exercise that implementation.
SET use_lightweight_primary_key_index_analysis = 1;

DROP TABLE IF EXISTS t_04350_lc;
DROP TABLE IF EXISTS t_04350_plain;
DROP TABLE IF EXISTS t_04350_merge;

-- The granule assertions below are layout-dependent, and the runner randomizes `index_granularity`
-- (1..65536) and `index_granularity_bytes`. An explicit per-DDL SETTINGS clause wins over that
-- injection (`ClientBase::addMergeTreeSettings` only adds a setting the CREATE does not already
-- carry), so pin both here instead of opting the whole test out of randomization.
CREATE TABLE t_04350_lc (k LowCardinality(UInt32), v String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
CREATE TABLE t_04350_plain (k UInt32, v String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
INSERT INTO t_04350_lc SELECT number, toString(number) FROM numbers(100000);
INSERT INTO t_04350_plain SELECT number, toString(number) FROM numbers(100000);

-- Merge header declares the plain type; sources mix LowCardinality and plain key columns.
CREATE TABLE t_04350_merge (k UInt32, v String)
    ENGINE = Merge(currentDatabase(), 't_04350_lc|t_04350_plain');

-- minus over the LowCardinality key inside KeyCondition (previously threw a LOGICAL_ERROR).
SELECT count() FROM t_04350_merge WHERE 3 = minus(materialize(materialize(65536)), k);
-- plus over the key.
SELECT count() FROM t_04350_merge WHERE 100003 = (k + materialize(3));
-- FINAL path.
SELECT DISTINCT count() FROM t_04350_merge FINAL WHERE (3 = minus(materialize(materialize(65536)), k)) AND notEmpty(v);

-- Correctness: PK pruning over the LowCardinality key must match the plain result.
SELECT count() FROM t_04350_merge WHERE (65536 - k) BETWEEN 3 AND 5003;
-- Liveness: a declined monotonic chain yields an unknown mask and a full scan, which would produce the
-- same counts as above, so the counts alone do not prove the chain ran. Assert pruning on the Merge
-- path this fix is about (not the direct table): count read nodes whose `Granules: <read>/<total>`
-- line reports read < total. The next query is the full-scan control on the same path, which must
-- report 0; together they show the oracle can distinguish pruning from a full scan.
SELECT countIf(extract(explain, 'Granules: ([0-9]+)/[0-9]+')::UInt64
               < extract(explain, 'Granules: [0-9]+/([0-9]+)')::UInt64)
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04350_merge WHERE (65536 - k) BETWEEN 3 AND 5003)
    WHERE extract(explain, 'Granules: ([0-9]+)/[0-9]+') != '';
SELECT countIf(extract(explain, 'Granules: ([0-9]+)/[0-9]+')::UInt64
               < extract(explain, 'Granules: [0-9]+/([0-9]+)')::UInt64)
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04350_merge WHERE notEmpty(v))
    WHERE extract(explain, 'Granules: ([0-9]+)/[0-9]+') != '';

DROP TABLE t_04350_lc;
DROP TABLE t_04350_plain;
DROP TABLE t_04350_merge;

-- Second path to the same class of exception: the sparse primary-key analysis. When the key column is
-- NOT loaded in the in-memory index (dropped as a useless suffix) but is bounded by the part's
-- partition minmax, it is analysed as a constant coordinate whose type comes from the raw key type.
-- For the LowCardinality source that raw type is LowCardinality, but the monotonic function chain was
-- built against the stripped type, so the sparse KeyCondition::checkInHyperrectangle caller fed a
-- LowCardinality type into a chain built on the plain type. With a CAST wrapper (here the implicit
-- UInt8->Bool cast) the dictionary-unpack step is elided, and applyFunctionForField builds a
-- LowCardinality const column that the inner cast wrapper then rejects with a Bad cast LOGICAL_ERROR.
-- The sparse caller must strip LowCardinality like the dense one.
DROP TABLE IF EXISTS t_04350_lc2;
DROP TABLE IF EXISTS t_04350_merge2;

-- Leading key column `a` is unique, so the useless suffix key column `b` is dropped from the
-- in-memory index; `PARTITION BY b` gives `b` a partition-minmax bound (constant coordinate).
CREATE TABLE t_04350_lc2 (a UInt64, b LowCardinality(Bool))
    ENGINE = MergeTree ORDER BY (a, b) PARTITION BY b
    SETTINGS index_granularity = 1, allow_nullable_key = 1,
             primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.5;
INSERT INTO t_04350_lc2 SELECT number, number % 2 = 0 FROM numbers(1000);

-- Merge header declares the plain Bool type over the LowCardinality(Bool) source.
CREATE TABLE t_04350_merge2 (a UInt64, b Bool)
    ENGINE = Merge(currentDatabase(), 't_04350_lc2');

-- CAST wrapper over the LowCardinality key reached via the sparse constant-coordinate path
-- (previously a Bad cast LOGICAL_ERROR). `b < 7` holds for every row, so this statement cannot assert
-- pruning; the count is its oracle, because before the fix it threw a LOGICAL_ERROR exception.
-- `use_partition_minmax_for_primary_key_pruning` defaults to 1 and the runner does not randomize it;
-- pin it anyway so the constant-coordinate bound on `b` is guaranteed present.
SELECT count() FROM t_04350_merge2 WHERE b < toLowCardinality(toNullable(7))
    SETTINGS use_partition_minmax_for_primary_key_pruning = 1;

-- The count above stays correct even if the chain declines and yields an unknown mask, so assert the
-- sparse chain's pruning too. Combining a selective leading-key predicate with the `b` chain leaves
-- Min-Max and Partition at their unpruned granule counts while PrimaryKey pruning depends on the
-- chain's result: applied it reads 52 of 500 granules, declined it would read the leading-key-only
-- 103. This statement also throws on an unfixed server.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1, actions = 0, pretty = 0
    SELECT count() FROM t_04350_merge2
    WHERE (a = 0 AND CAST(b, 'UInt8') = 1) OR (a >= 900 AND CAST(b, 'UInt8') = 1)
) WHERE explain LIKE '%Granules%' SETTINGS use_partition_minmax_for_primary_key_pruning = 1;

-- Same predicate with no index, to pin that the pruning above loses no rows.
SELECT count() FROM t_04350_merge2
    WHERE (a = 0 AND CAST(b, 'UInt8') = 1) OR (a >= 900 AND CAST(b, 'UInt8') = 1)
    SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE t_04350_lc2;
DROP TABLE t_04350_merge2;

-- Nested wrapper case: a Merge table with a plain Array(T) header over a source whose key is
-- Array(LowCardinality(T)). The monotonic function chain is built against the recursively-stripped
-- (plain) key type, so the strip at the execution site must also be recursive; a top-level-only
-- removeLowCardinality would leave a ColumnArray whose nested data is still ColumnLowCardinality and
-- feed it to arithmetic dispatched on the plain type. recursiveRemoveLowCardinality unwraps the
-- nested LowCardinality in lockstep with the type. (Through Merge the array CAST currently keeps this
-- off the applyFunction path, but the strip is aligned with the chain construction and prunes safely.)
DROP TABLE IF EXISTS t_04350_arr_lc;
DROP TABLE IF EXISTS t_04350_arr_merge;
CREATE TABLE t_04350_arr_lc (a Array(LowCardinality(Int64))) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04350_arr_lc VALUES ([1]), ([2]), ([3]);
CREATE TABLE t_04350_arr_merge (a Array(Int64)) ENGINE = Merge(currentDatabase(), 't_04350_arr_lc');
SELECT count() FROM t_04350_arr_merge
    WHERE plus(a, CAST([0] AS Array(Int16))) < CAST([3] AS Array(Int64));

DROP TABLE t_04350_arr_lc;
DROP TABLE t_04350_arr_merge;

-- Explicit-field path: a monotonic function chain whose intermediate function returns
-- LowCardinality. `applyMonotonicFunctionsChainToRange` propagates `current_type = result_type`,
-- so after `toLowCardinality(b)` the running type is LowCardinality again even though the caller
-- passed the recursively-stripped key type. On the partition-minmax constant-coordinate (explicit
-- field) path this reached `applyFunctionForField` with a LowCardinality `arg_type`, which built a
-- LowCardinality const column that the next function (a Bool CAST wrapper) then rejected with a Bad
-- cast LOGICAL_ERROR. `applyFunctionForField` must strip LowCardinality like the cached branch does.
DROP TABLE IF EXISTS t_04350_lc3;
DROP TABLE IF EXISTS t_04350_merge3;
CREATE TABLE t_04350_lc3 (a UInt64, b LowCardinality(Bool))
    ENGINE = MergeTree ORDER BY (a, b) PARTITION BY b
    SETTINGS index_granularity = 1, allow_nullable_key = 1,
             primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.5;
INSERT INTO t_04350_lc3 SELECT number, number % 2 = 0 FROM numbers(1000);
CREATE TABLE t_04350_merge3 (a UInt64, b Bool)
    ENGINE = Merge(currentDatabase(), 't_04350_lc3');
SELECT count() FROM t_04350_merge3 WHERE toLowCardinality(b) > toNullable(toLowCardinality(false));

-- Opposite direction on the same path: here the chain's running type is the plain key type while the
-- next function was resolved against a LowCardinality argument type, so building the const column on
-- the running type is also a bad cast (ColumnVector<char8_t> to ColumnLowCardinality). The const
-- column must follow the function's declared argument type, not the running type.
SELECT count() FROM t_04350_merge3 WHERE CAST(toLowCardinality(b), 'UInt64') > 0;
SELECT count() FROM t_04350_merge3 WHERE CAST(CAST(b, 'LowCardinality(UInt8)'), 'UInt64') > 0;
-- Both counts above are also what a declined chain would return, so assert that these two explicit-field
-- directions really prune (each reads one of the two partitions), with the full-scan control last.
-- The `toLowCardinality(b) > ...` case above is deliberately not asserted: it legitimately reads
-- everything, because `b` is bounded by the partition minmax in both partitions.
SELECT countIf(extract(explain, 'Granules: ([0-9]+)/[0-9]+')::UInt64
               < extract(explain, 'Granules: [0-9]+/([0-9]+)')::UInt64)
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04350_merge3
          WHERE CAST(toLowCardinality(b), 'UInt64') > 0)
    WHERE extract(explain, 'Granules: ([0-9]+)/[0-9]+') != '';
SELECT countIf(extract(explain, 'Granules: ([0-9]+)/[0-9]+')::UInt64
               < extract(explain, 'Granules: [0-9]+/([0-9]+)')::UInt64)
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04350_merge3
          WHERE CAST(CAST(b, 'LowCardinality(UInt8)'), 'UInt64') > 0)
    WHERE extract(explain, 'Granules: ([0-9]+)/[0-9]+') != '';
SELECT countIf(extract(explain, 'Granules: ([0-9]+)/[0-9]+')::UInt64
               < extract(explain, 'Granules: [0-9]+/([0-9]+)')::UInt64)
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04350_merge3 WHERE a >= 0)
    WHERE extract(explain, 'Granules: ([0-9]+)/[0-9]+') != '';

DROP TABLE t_04350_lc3;
DROP TABLE t_04350_merge3;

-- Same both-direction mismatch on the DENSE cached-column path: normal WHERE pruning builds
-- block-backed FieldRefs, so a two-link chain whose intermediate result type is LowCardinality reaches
-- `applyFunction`'s cache-miss branch, which strips the column to plain while the next link was
-- resolved against a LowCardinality argument type (previously a Bad cast LOGICAL_ERROR, on master too).
DROP TABLE IF EXISTS t_04350_lc4;
DROP TABLE IF EXISTS t_04350_merge4;
CREATE TABLE t_04350_lc4 (k LowCardinality(UInt16), v String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
INSERT INTO t_04350_lc4 SELECT number % 60000, toString(number) FROM numbers(100000);
CREATE TABLE t_04350_merge4 (k UInt16, v String) ENGINE = Merge(currentDatabase(), 't_04350_lc4');
SELECT count() FROM t_04350_merge4 WHERE CAST(CAST(k, 'LowCardinality(UInt16)'), 'UInt64') > 100;
-- The count above is also what a declined chain would return, so assert this dense cached path really
-- prunes, with the full-scan control on the same table next (must report 0).
SELECT countIf(extract(explain, 'Granules: ([0-9]+)/[0-9]+')::UInt64
               < extract(explain, 'Granules: [0-9]+/([0-9]+)')::UInt64)
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04350_merge4
          WHERE CAST(CAST(k, 'LowCardinality(UInt16)'), 'UInt64') > 59000)
    WHERE extract(explain, 'Granules: ([0-9]+)/[0-9]+') != '';
SELECT countIf(extract(explain, 'Granules: ([0-9]+)/[0-9]+')::UInt64
               < extract(explain, 'Granules: [0-9]+/([0-9]+)')::UInt64)
    FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04350_merge4 WHERE notEmpty(v))
    WHERE extract(explain, 'Granules: ([0-9]+)/[0-9]+') != '';

DROP TABLE t_04350_lc4;
DROP TABLE t_04350_merge4;
