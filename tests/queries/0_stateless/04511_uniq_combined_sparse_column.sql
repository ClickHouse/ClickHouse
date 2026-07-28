-- Regression test for the batched insertion path of `uniqCombined`/`uniqCombined64`
-- over sparse columns. In aggregation without keys all default rows of a sparse column
-- go through a single call of `addManyDefaults` and the non-default values go through
-- `addBatchSinglePlace` (see `addBatchSparseSinglePlace`), so the results and the
-- serialized states must be consistent with the dense path.

DROP TABLE IF EXISTS t_uniq_sparse;
DROP TABLE IF EXISTS t_uniq_dense;
DROP TABLE IF EXISTS t_uniq_states;

CREATE TABLE t_uniq_sparse
(
    id UInt64,
    n_small UInt64,
    n_medium UInt64,
    n_large UInt64,
    s_small String,
    s_medium String,
    s_large String,
    z UInt64
)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
    min_bytes_for_wide_part = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

CREATE TABLE t_uniq_dense
(
    id UInt64,
    n_small UInt64,
    n_medium UInt64,
    n_large UInt64,
    s_small String,
    s_medium String,
    s_large String,
    z UInt64
)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

-- 6 of every 7 rows are default. The period of non-default rows must not be a multiple
-- of 10: the ratio of default rows is estimated by sampling of every 10-th row and with
-- an aliasing pattern the column would not be serialized as sparse. Cardinalities are
-- chosen to exercise all container regimes of `CombinedCardinalityEstimator` for both
-- `uniqCombined(12)` and `uniqCombined64(12)`: small array (<= 16 elements), medium
-- hash set (<= 256/128 elements) and the large HyperLogLog. Column `z` consists of
-- defaults only.
INSERT INTO t_uniq_sparse SELECT
    number,
    if(number % 7 = 0, 1 + intDiv(number, 7) % 10, 0),
    if(number % 7 = 0, 1 + intDiv(number, 7) % 100, 0),
    if(number % 7 = 0, 1 + intDiv(number, 7), 0),
    if(number % 7 = 0, 'str_' || toString(intDiv(number, 7) % 10), ''),
    if(number % 7 = 0, 'str_' || toString(intDiv(number, 7) % 100), ''),
    if(number % 7 = 0, 'str_' || toString(intDiv(number, 7)), ''),
    0
FROM numbers(140000);

INSERT INTO t_uniq_dense SELECT * FROM t_uniq_sparse;

OPTIMIZE TABLE t_uniq_sparse FINAL;
OPTIMIZE TABLE t_uniq_dense FINAL;

SELECT 'serialization kinds';

SELECT DISTINCT table, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_uniq_sparse', 't_uniq_dense') AND active AND column != 'id'
ORDER BY table, column;

SELECT 'results, sparse vs dense';

SELECT 'n_small',
    (SELECT (uniqCombined(12)(n_small), uniqCombined64(12)(n_small), uniqCombined(n_small), uniqCombined64(n_small)) FROM t_uniq_sparse) AS res_sparse,
    (SELECT (uniqCombined(12)(n_small), uniqCombined64(12)(n_small), uniqCombined(n_small), uniqCombined64(n_small)) FROM t_uniq_dense) AS res_dense,
    res_sparse = res_dense;

SELECT 'n_medium',
    (SELECT (uniqCombined(12)(n_medium), uniqCombined64(12)(n_medium), uniqCombined(n_medium), uniqCombined64(n_medium)) FROM t_uniq_sparse) AS res_sparse,
    (SELECT (uniqCombined(12)(n_medium), uniqCombined64(12)(n_medium), uniqCombined(n_medium), uniqCombined64(n_medium)) FROM t_uniq_dense) AS res_dense,
    res_sparse = res_dense;

SELECT 'n_large',
    (SELECT (uniqCombined(12)(n_large), uniqCombined64(12)(n_large), uniqCombined(n_large), uniqCombined64(n_large)) FROM t_uniq_sparse) AS res_sparse,
    (SELECT (uniqCombined(12)(n_large), uniqCombined64(12)(n_large), uniqCombined(n_large), uniqCombined64(n_large)) FROM t_uniq_dense) AS res_dense,
    res_sparse = res_dense;

SELECT 's_small',
    (SELECT (uniqCombined(12)(s_small), uniqCombined64(12)(s_small), uniqCombined(s_small), uniqCombined64(s_small)) FROM t_uniq_sparse) AS res_sparse,
    (SELECT (uniqCombined(12)(s_small), uniqCombined64(12)(s_small), uniqCombined(s_small), uniqCombined64(s_small)) FROM t_uniq_dense) AS res_dense,
    res_sparse = res_dense;

SELECT 's_medium',
    (SELECT (uniqCombined(12)(s_medium), uniqCombined64(12)(s_medium), uniqCombined(s_medium), uniqCombined64(s_medium)) FROM t_uniq_sparse) AS res_sparse,
    (SELECT (uniqCombined(12)(s_medium), uniqCombined64(12)(s_medium), uniqCombined(s_medium), uniqCombined64(s_medium)) FROM t_uniq_dense) AS res_dense,
    res_sparse = res_dense;

SELECT 's_large',
    (SELECT (uniqCombined(12)(s_large), uniqCombined64(12)(s_large), uniqCombined(s_large), uniqCombined64(s_large)) FROM t_uniq_sparse) AS res_sparse,
    (SELECT (uniqCombined(12)(s_large), uniqCombined64(12)(s_large), uniqCombined(s_large), uniqCombined64(s_large)) FROM t_uniq_dense) AS res_dense,
    res_sparse = res_dense;

SELECT 'z',
    (SELECT (uniqCombined(12)(z), uniqCombined64(12)(z), uniqCombined(z), uniqCombined64(z)) FROM t_uniq_sparse) AS res_sparse,
    (SELECT (uniqCombined(12)(z), uniqCombined64(12)(z), uniqCombined(z), uniqCombined64(z)) FROM t_uniq_dense) AS res_dense,
    res_sparse = res_dense;

-- The serialized state may be compared byte-wise between the sparse and the dense path
-- only when it does not depend on the insertion order: the sparse path adds defaults
-- after all non-default values, while small and medium containers serialize elements
-- in the insertion order. The HyperLogLog state is a pure function of the inserted set,
-- and for `z` the state contains a single element.
SELECT 'state bytes, sparse vs dense';

SELECT 'n_large',
    (SELECT hex(uniqCombinedState(12)(n_large)) FROM t_uniq_sparse) = (SELECT hex(uniqCombinedState(12)(n_large)) FROM t_uniq_dense),
    (SELECT hex(uniqCombined64State(12)(n_large)) FROM t_uniq_sparse) = (SELECT hex(uniqCombined64State(12)(n_large)) FROM t_uniq_dense),
    (SELECT hex(uniqCombinedState(n_large)) FROM t_uniq_sparse) = (SELECT hex(uniqCombinedState(n_large)) FROM t_uniq_dense),
    (SELECT hex(uniqCombined64State(n_large)) FROM t_uniq_sparse) = (SELECT hex(uniqCombined64State(n_large)) FROM t_uniq_dense);

SELECT 's_large',
    (SELECT hex(uniqCombinedState(12)(s_large)) FROM t_uniq_sparse) = (SELECT hex(uniqCombinedState(12)(s_large)) FROM t_uniq_dense),
    (SELECT hex(uniqCombined64State(12)(s_large)) FROM t_uniq_sparse) = (SELECT hex(uniqCombined64State(12)(s_large)) FROM t_uniq_dense),
    (SELECT hex(uniqCombinedState(s_large)) FROM t_uniq_sparse) = (SELECT hex(uniqCombinedState(s_large)) FROM t_uniq_dense),
    (SELECT hex(uniqCombined64State(s_large)) FROM t_uniq_sparse) = (SELECT hex(uniqCombined64State(s_large)) FROM t_uniq_dense);

SELECT 'z',
    (SELECT hex(uniqCombinedState(12)(z)) FROM t_uniq_sparse) = (SELECT hex(uniqCombinedState(12)(z)) FROM t_uniq_dense),
    (SELECT hex(uniqCombined64State(12)(z)) FROM t_uniq_sparse) = (SELECT hex(uniqCombined64State(12)(z)) FROM t_uniq_dense),
    (SELECT hex(uniqCombinedState(z)) FROM t_uniq_sparse) = (SELECT hex(uniqCombinedState(z)) FROM t_uniq_dense),
    (SELECT hex(uniqCombined64State(z)) FROM t_uniq_sparse) = (SELECT hex(uniqCombined64State(z)) FROM t_uniq_dense);

-- States produced from a sparse column must survive the round-trip through the on-disk
-- serialization and must merge correctly with states produced from a dense column.
CREATE TABLE t_uniq_states
(
    label String,
    st_n AggregateFunction(uniqCombined(12), UInt64),
    st_s AggregateFunction(uniqCombined(12), String),
    st_n64 AggregateFunction(uniqCombined64(12), UInt64),
    st_s64 AggregateFunction(uniqCombined64(12), String)
)
ENGINE = MergeTree ORDER BY label;

INSERT INTO t_uniq_states SELECT 'small_sparse', uniqCombinedState(12)(n_small), uniqCombinedState(12)(s_small), uniqCombined64State(12)(n_small), uniqCombined64State(12)(s_small) FROM t_uniq_sparse;
INSERT INTO t_uniq_states SELECT 'small_dense', uniqCombinedState(12)(n_small), uniqCombinedState(12)(s_small), uniqCombined64State(12)(n_small), uniqCombined64State(12)(s_small) FROM t_uniq_dense;
INSERT INTO t_uniq_states SELECT 'medium_sparse', uniqCombinedState(12)(n_medium), uniqCombinedState(12)(s_medium), uniqCombined64State(12)(n_medium), uniqCombined64State(12)(s_medium) FROM t_uniq_sparse;
INSERT INTO t_uniq_states SELECT 'medium_dense', uniqCombinedState(12)(n_medium), uniqCombinedState(12)(s_medium), uniqCombined64State(12)(n_medium), uniqCombined64State(12)(s_medium) FROM t_uniq_dense;
INSERT INTO t_uniq_states SELECT 'large_sparse', uniqCombinedState(12)(n_large), uniqCombinedState(12)(s_large), uniqCombined64State(12)(n_large), uniqCombined64State(12)(s_large) FROM t_uniq_sparse;
INSERT INTO t_uniq_states SELECT 'large_dense', uniqCombinedState(12)(n_large), uniqCombinedState(12)(s_large), uniqCombined64State(12)(n_large), uniqCombined64State(12)(s_large) FROM t_uniq_dense;

SELECT 'states after round-trip';

SELECT label, finalizeAggregation(st_n), finalizeAggregation(st_s), finalizeAggregation(st_n64), finalizeAggregation(st_s64)
FROM t_uniq_states ORDER BY label;

SELECT 'merge of states from sparse and dense paths';

SELECT splitByChar('_', label)[1] AS regime,
    uniqCombinedMerge(12)(st_n), uniqCombinedMerge(12)(st_s), uniqCombined64Merge(12)(st_n64), uniqCombined64Merge(12)(st_s64)
FROM t_uniq_states GROUP BY regime ORDER BY regime;

DROP TABLE t_uniq_sparse;
DROP TABLE t_uniq_dense;
DROP TABLE t_uniq_states;
