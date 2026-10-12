#!/usr/bin/env bash

# A per-request `default_format` URL parameter must win over clickhouse-local's display default on the
# embedded HTTP listener (`SYSTEM START LISTEN HTTP`). clickhouse-local seeds its display default into
# `global_context`, and that context is inherited by the listener's per-request contexts. The default
# must therefore be the `default_format` *setting* (a fallback consulted only after the request's own
# value) rather than the legacy `Context::default_format` field — `Context::getDefaultFormat` checks
# the legacy field first, so seeding it would mask the request's `?default_format=`. On
# `clickhouse-server` the legacy field is empty and the request value already wins; this test pins the
# clickhouse-local behavior to match.
#
# Use OS-assigned ports (`--http_port 0`) to avoid collisions with parallel CI jobs, then read the
# listener's response with `url(..., LineAsString)` to inspect the raw output bytes: CSV quotes the
# string (`"hello"`), the local non-interactive display default (TSV) does not (`hello`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 \
    --http_port 0 \
    --query "
    SYSTEM START LISTEN HTTP;
    SELECT '-- default_format=CSV wins over the local display default';
    SELECT * FROM url('http://127.0.0.1:' || toString(getServerPort('http_port')) || '/?default_format=CSV&query=' || encodeURLComponent('SELECT ''hello'' AS s'), LineAsString);
    SELECT '-- default_format=JSONEachRow is honored too';
    SELECT * FROM url('http://127.0.0.1:' || toString(getServerPort('http_port')) || '/?default_format=JSONEachRow&query=' || encodeURLComponent('SELECT ''hello'' AS s'), LineAsString);
    SELECT '-- no override: the local display default (TSV) is still the fallback';
    SELECT * FROM url('http://127.0.0.1:' || toString(getServerPort('http_port')) || '/?query=' || encodeURLComponent('SELECT ''hello'' AS s'), LineAsString);
    SYSTEM STOP LISTEN HTTP;
"
