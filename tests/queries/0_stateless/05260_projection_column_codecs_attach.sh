#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_projection_codec_attach"
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
attach_query="ATTACH TABLE t_projection_codec_attach UUID '${uuid}'
(
    k UInt64,
    x UInt64,
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k"

# A full-definition ATTACH is fresh input, so its codec is checked against session settings.
$CLICKHOUSE_CLIENT --send_logs_level fatal --allow_suspicious_codecs 0 -q "$attach_query" 2>&1 | grep -q -F 'BAD_ARGUMENTS'

$CLICKHOUSE_CLIENT --send_logs_level fatal --allow_suspicious_codecs 1 -q "$attach_query"

# Reattaching stored metadata does not depend on the original session setting.
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_projection_codec_attach"
$CLICKHOUSE_CLIENT --allow_suspicious_codecs 0 -q "ATTACH TABLE t_projection_codec_attach"
$CLICKHOUSE_CLIENT -q "SELECT 'attached', codecs FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_codec_attach'"

# RESTORE loads previously accepted metadata, including codecs on ordinary and projection
# columns, without depending on the current session's suspicious-codec setting.
$CLICKHOUSE_CLIENT --allow_suspicious_codecs 1 -q "ALTER TABLE t_projection_codec_attach MODIFY COLUMN x CODEC(Gorilla)"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_projection_codec_attach TO Memory('${CLICKHOUSE_DATABASE}_projection_codec_restore') FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_projection_codec_attach SYNC"
$CLICKHOUSE_CLIENT --allow_suspicious_codecs 0 -q "RESTORE TABLE t_projection_codec_attach FROM Memory('${CLICKHOUSE_DATABASE}_projection_codec_restore') FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT 'restored_projection', codecs FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_codec_attach'"
$CLICKHOUSE_CLIENT -q "SELECT 'restored_column', countIf(position(compression_codec, 'Gorilla') > 0) FROM system.columns WHERE database = currentDatabase() AND table = 't_projection_codec_attach' AND name = 'x'"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_projection_codec_attach"
