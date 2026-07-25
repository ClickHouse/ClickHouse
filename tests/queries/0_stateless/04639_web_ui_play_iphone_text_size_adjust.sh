#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for broken SQL syntax highlighting in the Web UI on iPhone.
# Mobile Safari's automatic text inflation rescaled the `#query` textarea and the
# `#query-backdrop` div behind it by different multipliers, misaligning the
# highlighting overlay. The fix disables the inflation with `text-size-adjust: 100%`
# and adds a `device-width` viewport, compensating the focus-zoom side effect by
# raising form-control fonts to 16px on touch devices.

PLAY_PAGE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play")

echo "$PLAY_PAGE" | grep -o -F -m1 -- '-webkit-text-size-adjust: 100%'
echo "$PLAY_PAGE" | grep -o -F -m1 '<meta name="viewport" content="width=device-width, initial-scale=1">'
echo "$PLAY_PAGE" | grep -o -F -m1 '@media (hover: none) and (pointer: coarse)'
