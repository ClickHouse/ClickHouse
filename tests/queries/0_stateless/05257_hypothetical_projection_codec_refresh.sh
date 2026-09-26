#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "--- a disabled codec gate makes the candidate inapplicable ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_codec_gate;
    CREATE TABLE t_hypo_codec_gate (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_hypo_codec_gate SELECT number, number FROM numbers(100);
    SET allow_suspicious_codecs = 1;
    CREATE HYPOTHETICAL PROJECTION p_gate ON t_hypo_codec_gate
        (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY x);
    SET allow_suspicious_codecs = 0;
    EXPLAIN WHATIF SELECT x FROM t_hypo_codec_gate WHERE x = 1;
    DROP TABLE t_hypo_codec_gate;
" 2>&1 | grep -oE 'With p_gate \(normal projection, hypothetical\):|^[[:space:]]+status: +[a-z_]+|reason: +Hypothetical projection can no longer be added to this table' | awk '{$1=$1; print}'

echo "--- a changed resolved type makes the candidate inapplicable ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_codec_type;
    CREATE TABLE t_hypo_codec_type (k UInt64, x Float64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_hypo_codec_type SELECT number, toFloat64(number) FROM numbers(100);
    CREATE HYPOTHETICAL PROJECTION p_type ON t_hypo_codec_type
        (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY x);
    ALTER TABLE t_hypo_codec_type MODIFY COLUMN x UInt64 SETTINGS mutations_sync = 2;
    EXPLAIN WHATIF SELECT x FROM t_hypo_codec_type WHERE x = 1;
    DROP TABLE t_hypo_codec_type;
" 2>&1 | grep -oE 'With p_type \(normal projection, hypothetical\):|^[[:space:]]+status: +[a-z_]+|reason: +Hypothetical projection can no longer be added to this table' | awk '{$1=$1; print}'
