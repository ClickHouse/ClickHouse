-- Tags: no-fasttest
-- no-fasttest: needs the server-side AST fuzzer (ast_fuzzer_runs)

-- Regression test for #109706: the fuzzer must not inject expressions into data-type argument lists, since
-- an ASTDataType carrying an ASTFunction argument formats to text that parses back differently and trips the
-- consistency check in executeQuery. Runs the fuzzer over a CREATE query covering the argument-bearing types.

SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 20;
SET ast_fuzzer_any_query = 1;

CREATE TABLE t_04344 (a Nullable(Int32), b LowCardinality(String), c Array(Nullable(UInt64)), d Map(String, Int64), e FixedString(8), f JSON(max_dynamic_paths=8, p1 UInt32, p2 Array(String), SKIP s, SKIP REGEXP '^sk.*$'), g QBit(Float32, 16), h SimpleAggregateFunction(sumMap, Tuple(Array(String), Array(UInt64))), n SimpleAggregateFunction(sum, UInt64), i DateTime('Asia/Istanbul'), j DateTime64(3, 'UTC'), k Nested(x UInt32, y Array(Nullable(Int64))), l AggregateFunction(quantileExact(0.5), Float64), m AggregateFunction(topK(10), String), o Point, p Ring, w MultiPoint, q Polygon, r MultiPolygon, s LineString, u MultiLineString, v Geometry) ENGINE = Memory;

SELECT 1;
