#!/usr/bin/env bash

# `clickhouse-local --output-format` (like `--format` and `--input-format`) describes how the local
# client itself reads its input and prints its results. It is mirrored into the `output_format`
# setting so it travels with every query the client runs - but it must stay private to that client:
# `global_context` is also inherited by the sessions of the embedded protocol listeners
# (`SYSTEM START LISTEN HTTP`), and there `output_format` would become a strong per-request override,
# so a remote client asking for `?default_format=JSONEachRow` would still be answered in the local
# CLI's format.
#
# The local display default is still offered to those sessions, but only through the weaker
# `default_format` fallback, which applies when the request selects no format of its own.
#
# Use an OS-assigned port (`--http_port 0`) to avoid collisions with parallel CI jobs, and read the
# listener's response with `url(..., LineAsString)` to inspect the raw response bytes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 \
    --http_port 0 \
    --output-format CSV \
    --query "
    SYSTEM START LISTEN HTTP;

    SELECT if(line = '{\"s\":\"hello\"}', 'the request default_format wins over the local --output-format', 'FAIL: ' || line)
    FROM url('http://127.0.0.1:' || toString(getServerPort('http_port')) || '/?default_format=JSONEachRow&query=' || encodeURLComponent('SELECT ''hello'' AS s'), LineAsString);

    SELECT if(line = '\"hello\"', 'the local display default is still the fallback', 'FAIL: ' || line)
    FROM url('http://127.0.0.1:' || toString(getServerPort('http_port')) || '/?query=' || encodeURLComponent('SELECT ''hello'' AS s'), LineAsString);

    SYSTEM STOP LISTEN HTTP;
"
