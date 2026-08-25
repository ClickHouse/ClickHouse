#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Swapped NEAREST must not clone unused probe vectors into NearestSwapState.
# 8000 build rows of Array(Float32) length 4096 are 128 MiB. Copying that
# column for an ids-only SELECT makes ids-only memory match ids+vecs memory.
# Skipping it makes the ids+vecs query take at least one extra vector copy.
# The ids+vecs query must consume the vectors in the outer SELECT; otherwise
# the analyzer drops them from the join output and both queries skip.

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS unused_vec_base;
DROP TABLE IF EXISTS unused_vec_upload;
CREATE TABLE unused_vec_base (k UInt32, base_id UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE unused_vec_upload (query_id UInt32, k UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY query_id;
INSERT INTO unused_vec_upload
SELECT number, number % 2000, arrayMap(i -> toFloat32(i + number), range(4096))
FROM numbers(8000);
INSERT INTO unused_vec_base
SELECT number % 2000, number, arrayMap(i -> toFloat32(i + number + 1), range(4096))
FROM numbers(20000);
"

run_tagged()
{
    local tag="$1"
    local query="$2"
    local result_json
    result_json=$($CLICKHOUSE_CLIENT --log_queries=1 -q "$query")
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
    local memory_json
    memory_json=$($CLICKHOUSE_CLIENT -q "
SELECT memory_usage
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%${tag}%'
  AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1
FORMAT JSONEachRow
")
    python3 -c 'import json,sys; row=json.loads(sys.argv[1]); row["memory_usage"]=json.loads(sys.argv[2])["memory_usage"]; print(json.dumps(row))' "$result_json" "$memory_json"
}

COMMON="query_plan_join_swap_table = 1, join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8, log_queries = 1"

ids_json=$(run_tagged "04912_ids_only" "
SELECT /* 04912_ids_only */
    count() AS matched,
    sum(cityHash64(query_id, base_id)) AS checksum
FROM
(
    SELECT upload.query_id AS query_id, base.base_id AS base_id
    FROM unused_vec_upload AS upload
    NEAREST JOIN unused_vec_base AS base
        ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS ${COMMON}
FORMAT JSONEachRow
")

vecs_json=$(run_tagged "04912_ids_and_vecs" "
SELECT /* 04912_ids_and_vecs */
    count() AS matched,
    sum(cityHash64(query_id, base_id)) AS checksum,
    round(sum(L2Distance(upload_vec, base_vec)), 3) AS distance_sum
FROM
(
    SELECT
        upload.query_id AS query_id,
        base.base_id AS base_id,
        upload.vec AS upload_vec,
        base.vec AS base_vec
    FROM unused_vec_upload AS upload
    NEAREST JOIN unused_vec_base AS base
        ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS ${COMMON}
FORMAT JSONEachRow
")

hash_json=$(run_tagged "04912_hash_ids" "
SELECT /* 04912_hash_ids */
    count() AS matched,
    sum(cityHash64(query_id, base_id)) AS checksum
FROM
(
    SELECT upload.query_id AS query_id, base.base_id AS base_id
    FROM unused_vec_upload AS upload
    NEAREST JOIN unused_vec_base AS base
        ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS query_plan_join_swap_table = 1, join_algorithm = 'hash', max_threads = 8, log_queries = 1
FORMAT JSONEachRow
")

$CLICKHOUSE_CLIENT -q "DROP TABLE unused_vec_base; DROP TABLE unused_vec_upload;"

python3 - "$ids_json" "$vecs_json" "$hash_json" <<'PY'
import json
import sys

ids = json.loads(sys.argv[1])
vecs = json.loads(sys.argv[2])
hashed = json.loads(sys.argv[3])
one_vector_copy = 8000 * 4096 * 4
print("parallel_hash ids-only matches hash:", int(ids["matched"] == hashed["matched"] and ids["checksum"] == hashed["checksum"]))
print("ids-only matches ids+vecs:", int(ids["matched"] == vecs["matched"] and ids["checksum"] == vecs["checksum"]))
print(
    "unused probe vectors skipped:",
    int(int(vecs["memory_usage"]) - int(ids["memory_usage"]) >= one_vector_copy // 2),
)
PY
