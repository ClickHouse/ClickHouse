-- The aggregate functions that accept a Variant argument natively skip the rows where the Variant holds a NULL
-- value. That is a change of behavior, so `aggregate_functions_skip_variant_nulls = 0` (implied by
-- `compatibility` below 26.9) restores the previous behavior, where those rows were aggregated as ordinary values.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_compat;
CREATE TABLE t_variant_compat (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_compat VALUES (NULL), (1::UInt64), (NULL), ('a');

-- The `compatibility` setting picks the old behavior as long as the setting itself is not set explicitly.
SET compatibility = '26.8';
SELECT 'compatibility 26.8', count(v), uniqExact(v), any(v), length(groupArray(v)) FROM t_variant_compat;
SET compatibility = '';

SET aggregate_functions_skip_variant_nulls = 1;
SELECT 'skip', count(v), uniqExact(v), any(v), length(groupArray(v)) FROM t_variant_compat;

SET aggregate_functions_skip_variant_nulls = 0;
SELECT 'keep', count(v), uniqExact(v), any(v), length(groupArray(v)) FROM t_variant_compat;

-- The state representation is the same in both modes, so a state written with one value of the setting is
-- readable (and mergeable) with the other. Only the values that went into it differ.
SET aggregate_functions_skip_variant_nulls = 1;
SELECT 'state type skip', toTypeName(countState(v)), toTypeName(anyState(v)) FROM t_variant_compat;
SET aggregate_functions_skip_variant_nulls = 0;
SELECT 'state type keep', toTypeName(countState(v)), toTypeName(anyState(v)) FROM t_variant_compat;

DROP TABLE IF EXISTS t_variant_compat_states;
CREATE TABLE t_variant_compat_states (s AggregateFunction(count, Variant(UInt64, String))) ENGINE = Memory;

SET aggregate_functions_skip_variant_nulls = 0;
INSERT INTO t_variant_compat_states SELECT countState(v) FROM t_variant_compat;
SET aggregate_functions_skip_variant_nulls = 1;
INSERT INTO t_variant_compat_states SELECT countState(v) FROM t_variant_compat;

SELECT 'merge of both states', countMerge(s) FROM t_variant_compat_states;

-- The setting does not affect the supertype adapter: those functions rejected a Variant argument before, so there
-- is nothing to stay compatible with, and the NULLs are skipped by the Nullable(supertype) cast in either mode.
SET aggregate_functions_skip_variant_nulls = 0;
SELECT 'adapter', sum(v), min(v) FROM
(
    SELECT CAST(number % 2 ? NULL : toInt32(number) AS Variant(Int32, Float64)) AS v FROM numbers(4)
);

DROP TABLE t_variant_compat_states;
DROP TABLE t_variant_compat;
