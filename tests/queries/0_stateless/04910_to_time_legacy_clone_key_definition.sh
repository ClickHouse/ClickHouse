#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `CREATE CLONE AS is not supported with Replicated databases`
# A clone copies its source definition out of stored metadata, so the spelling the source declared
# has to survive verbatim even when the cloning session asks for the legacy one.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --multiquery -q "
SET use_legacy_to_time = 0;
CREATE TABLE t_totime_clone_source (c0 DateTime, c1 UInt32)
ENGINE = MergeTree() ORDER BY (toUInt32(toTime(c0)), c1);
INSERT INTO t_totime_clone_source VALUES ('2026-01-01 01:02:03', 1);
SET use_legacy_to_time = 1;
CREATE TABLE t_totime_clone CLONE AS t_totime_clone_source;
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_clone';
SELECT count() FROM t_totime_clone;
DROP TABLE t_totime_clone;
DROP TABLE t_totime_clone_source;
"
