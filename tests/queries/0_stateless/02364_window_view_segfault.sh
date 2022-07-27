#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --multiline --query """
DROP TABLE IF EXISTS mt ON CLUSTER test_shard_localhost;
DROP TABLE IF EXISTS wv ON CLUSTER test_shard_localhost;
CREATE TABLE mt  ON CLUSTER test_shard_localhost (a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv ON CLUSTER test_shard_localhost TO input_deduplicated INNER ENGINE Memory WATERMARK=INTERVAL '1' SECOND AS SELECT count(a), hopStart(wid) AS w_start, hopEnd(wid) AS w_end FROM mt GROUP BY hop(timestamp, INTERVAL '3' SECOND, INTERVAL '5' SECOND) AS wid;
""" 2>&1 | grep -q -e "Code: 344" -e "Code: 60" && echo 'ok' || echo 'fail' ||:
