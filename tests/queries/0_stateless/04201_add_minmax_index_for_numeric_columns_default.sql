-- Verify that `add_minmax_index_for_numeric_columns` is on by default,
-- and that the opt-out (`add_minmax_index_for_numeric_columns = 0`) still works.

DROP TABLE IF EXISTS t_implicit_minmax_default;
DROP TABLE IF EXISTS t_implicit_minmax_disabled;

-- Default: numeric columns should each get an automatic min-max skipping index;
-- non-numeric columns should not.
CREATE TABLE t_implicit_minmax_default
(
    id UInt64,
    s String,
    x Int32,
    y Float64,
    d Date
)
ENGINE = MergeTree
ORDER BY id;

SELECT 'default - implicit minmax index columns', name, expr
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_implicit_minmax_default' AND creation = 'Implicit'
ORDER BY name;

-- Opt-out: no automatic indices should be created.
CREATE TABLE t_implicit_minmax_disabled
(
    id UInt64,
    s String,
    x Int32,
    y Float64,
    d Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;

SELECT 'disabled - implicit minmax index count', count()
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_implicit_minmax_disabled' AND creation = 'Implicit';

DROP TABLE t_implicit_minmax_default;
DROP TABLE t_implicit_minmax_disabled;
