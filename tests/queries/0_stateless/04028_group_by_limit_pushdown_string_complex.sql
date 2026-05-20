-- Tests for correctness of the enable_group_by_top_k_optimization optimization.
-- Part 2: string, complex, nullable, and low-cardinality key types.

-- Tags: no-parallel-replicas, long

SET enable_group_by_top_k_optimization = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_gbylimit;

CREATE TABLE t_gbylimit
(
    k_str String,
    k_fstr FixedString(12),
    k_tup Tuple(UInt32, UInt32),
    k_nu32 Nullable(UInt32),
    k_nstr Nullable(String),
    k_lcu64 LowCardinality(UInt64),
    k_lcstr LowCardinality(String),
    val UInt64
) ENGINE = MergeTree ORDER BY k_str;

INSERT INTO t_gbylimit
SELECT
    toString(number % 30000),
    toFixedString(toString(number % 25000), 12),
    tuple((number % 20000)::UInt32, ((number * 3) % 20000)::UInt32),
    if(number % 97 = 0, NULL, (number % 35000)::UInt32),
    if(number % 83 = 0, NULL, toString(number % 30000)),
    number % 45000,
    toString(number % 28000),
    number
FROM numbers(50000);

-- =====================
-- key_string (String)
-- =====================
SELECT 'key_string';
SELECT k_str, count(), sum(val)
FROM t_gbylimit GROUP BY k_str ORDER BY k_str ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_str, count(), sum(val)
FROM t_gbylimit GROUP BY k_str ORDER BY k_str ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- key_fixed_string (FixedString)
-- =====================
SELECT 'key_fixed_string';
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- serialized (Tuple)
-- =====================
SELECT 'serialized';
SELECT k_tup, count(), sum(val)
FROM t_gbylimit GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_tup, count(), sum(val)
FROM t_gbylimit GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- nullable_key32 (Nullable(UInt32))
-- =====================
SELECT 'nullable_key32';
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- nullable_key_string (Nullable(String))
-- =====================
SELECT 'nullable_key_string';
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- low_cardinality_key64 (LowCardinality(UInt64))
-- =====================
SELECT 'low_cardinality_key64';
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- low_cardinality_key_string (LowCardinality(String))
-- =====================
SELECT 'low_cardinality_key_string';
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit;
