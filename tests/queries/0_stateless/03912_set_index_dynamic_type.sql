-- Regression test: set index on Dynamic column with indexHint should not cause
-- "ColumnUInt8 is expected as a Set index condition result" exception.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=a1ce95512f6c8d75e10929bc4835787e194eeed8&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_ubsan%29

SET allow_suspicious_indices = 1;
SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS t_set_index_dynamic;
CREATE TABLE t_set_index_dynamic (k Float32, v Dynamic(max_types = 2), INDEX i v TYPE set(100) GRANULARITY 2) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, index_granularity_bytes = 1000000000, min_index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_set_index_dynamic SELECT number, intDiv(number, 4096) FROM numbers(10000);

SELECT count() FROM t_set_index_dynamic WHERE indexHint(v);
SELECT count() FROM t_set_index_dynamic WHERE indexHint(indexHint(*));
SELECT count() FROM t_set_index_dynamic PREWHERE indexHint(indexHint(*)) WHERE indexHint(indexHint(*));

DROP TABLE t_set_index_dynamic;
