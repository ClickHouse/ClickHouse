#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The read stats of the Web UI progress line wrap the outstanding numbers - huge row and byte counts
# and high read speeds - in `<b>`, and a CSS rule paints them in a color. That line paints all of its
# text through a `background-clip: text` mask and makes the text itself transparent to reveal it, and
# `-webkit-text-fill-color` is inherited, so a rule setting only `color` has no effect at all and the
# highlight is silently lost. Check that the numbers are wrapped and that both properties are set.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PLAY=$(${CLICKHOUSE_CURL} -sS "${URL}/play")

printf '%s' "$PLAY" | grep -cF '<b>${formatted_rps}<\/b>'
printf '%s' "$PLAY" | tr -d ' \n' | grep -oF '#statsb{color:var(--metric-c6);-webkit-text-fill-color:var(--metric-c6);}' | head -n1
