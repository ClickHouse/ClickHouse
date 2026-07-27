-- Repro for https://github.com/ClickHouse/ClickHouse/issues/109811
-- Join runtime filter used to throw NOT_IMPLEMENTED ("Method getDataAt is not supported for Nullable(String)")
-- when the build-side key was LowCardinality(Nullable(...)) with real NULLs and the filter overflowed the
-- exact-values set into the bloom filter. A small join_runtime_filter_exact_values_limit forces that overflow.

DROP TABLE IF EXISTS b_lcn;
DROP TABLE IF EXISTS s_lcn;

CREATE TABLE b_lcn (k LowCardinality(Nullable(String)), v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE s_lcn (k LowCardinality(Nullable(String)), name String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO b_lcn SELECT if(number % 97 = 0, NULL, toString(number % 5000)), number FROM numbers(200000);
INSERT INTO s_lcn SELECT if(number % 7 = 0, NULL, toString(number * 33)), toString(number) FROM numbers(150);

-- Forced overflow into the bloom filter via a tiny exact-values limit.
SELECT count() FROM b_lcn AS b JOIN s_lcn AS s ON b.k = s.k
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 4;

-- Same query without runtime filters must give the same result (NULLs never match in a JOIN key).
SELECT count() FROM b_lcn AS b JOIN s_lcn AS s ON b.k = s.k
SETTINGS enable_join_runtime_filters = 0;

-- LowCardinality(String) (non-nullable dictionary) must still be routed through the bloom filter and work.
SELECT count() FROM
  (SELECT toString(number % 5000)::LowCardinality(String) AS k FROM numbers(200000)) AS b
  JOIN
  (SELECT toString(number * 33)::LowCardinality(String) AS k FROM numbers(150)) AS s
  ON b.k = s.k
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 4;

DROP TABLE b_lcn;
DROP TABLE s_lcn;
