#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

use_pdx=$($CLICKHOUSE_CLIENT -q "SELECT toUInt8(any(value) IN ('1', 'ON', 'TRUE')) FROM system.build_options WHERE name='USE_PDX'")
if [[ "$use_pdx" != "1" ]]; then
    exit 0
fi

$CLICKHOUSE_CLIENT -nmq "
    DROP TABLE IF EXISTS tab_pdx_smoke;

    CREATE TABLE tab_pdx_smoke
    (
        id UInt64,
        vec Array(Float32),
        INDEX idx vec TYPE vector_similarity('pdx', 'cosineDistance', 2)
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity = 2;

    INSERT INTO tab_pdx_smoke VALUES
        (0, [1.0, 0.0]),
        (1, [0.9, 0.1]),
        (2, [0.0, 1.0]),
        (3, [0.1, 0.9]);

    SELECT count()
    FROM
    (
        SELECT id
        FROM tab_pdx_smoke
        ORDER BY cosineDistance(vec, [1.0, 0.0])
        LIMIT 2
    )
    SETTINGS vector_search_with_rescoring = 1,
             hnsw_candidate_list_size_for_search = 2;

    DROP TABLE tab_pdx_smoke;
"
