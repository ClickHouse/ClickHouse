#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: vector_similarity needs usearch, absent in the ENABLE_LIBRARIES=0 fast build
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_noargs;
    CREATE TABLE t_hypo_noargs (a UInt64, b String, v Array(Float32)) ENGINE = MergeTree ORDER BY a;
"

# Arg-taking index types with the argument omitted used to reach the index creator
# (which reads its arguments unguarded) before validation and crash the server.
# They must now be rejected cleanly by the validator. Covers the whole crash family:
# set/ngrambf_v1/tokenbf_v1/sparse_grams (bloom-filter creators) and vector_similarity.

echo "--- set with no argument ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_noargs (b) TYPE set GRANULARITY 1;" 2>&1 | grep -m1 -oE 'INCORRECT_QUERY|BAD_ARGUMENTS'

echo "--- ngrambf_v1 with no arguments ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_noargs (b) TYPE ngrambf_v1 GRANULARITY 1;" 2>&1 | grep -m1 -oE 'INCORRECT_QUERY|BAD_ARGUMENTS'

echo "--- tokenbf_v1 with no arguments ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_noargs (b) TYPE tokenbf_v1 GRANULARITY 1;" 2>&1 | grep -m1 -oE 'INCORRECT_QUERY|BAD_ARGUMENTS'

echo "--- sparse_grams with no arguments ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_noargs (b) TYPE sparse_grams GRANULARITY 1;" 2>&1 | grep -m1 -oE 'INCORRECT_QUERY|BAD_ARGUMENTS'

echo "--- vector_similarity with no arguments ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_noargs (v) TYPE vector_similarity GRANULARITY 1;" 2>&1 | grep -m1 -oE 'INCORRECT_QUERY|BAD_ARGUMENTS'

echo "--- server is still alive ---"
$CLICKHOUSE_CLIENT -q "SELECT 1"

echo "--- well-formed set is still accepted ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi ON t_hypo_noargs (b) TYPE set(100) GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_noargs' AND name = 'hi';
"

# A well-formed but unsupported type must still pass validation and then be rejected by
# the explicit "not supported" branch (validate before construct preserves author intent).
echo "--- well-formed vector_similarity is still 'not supported' ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX hi ON t_hypo_noargs (v) TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1;" 2>&1 | grep -m1 -oE 'NOT_IMPLEMENTED'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_noargs;"
