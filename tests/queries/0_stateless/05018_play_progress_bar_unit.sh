#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The progress line of the Web UI paints its text through a gradient text mask keyed to the
# `--progress` custom property: the text itself is transparent and shows only through that mask.
# `--progress` is substituted into the color stops of the gradients and into a `calc`, where a
# unitless `0` is a `<number>` instead of a `<length-percentage>`, which makes the gradients - and
# with them the whole `background` - invalid. The mask then disappears and the line, including the
# final stats of a finished query ("130 rows in result, 0.14 sec. ... Read 237.31 M rows, ..."), is
# rendered in an invisible color. So every value assigned to `--progress` must carry a unit.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# The count of assignments of a literal without a unit is zero.
${CLICKHOUSE_CURL} -sS "${URL}/play" | grep -cE "setProperty\('--progress', *'[^%']*'\)" ||:

# The reset applied once a query has finished goes through `clearBar`, which carries the unit, so
# the check above cannot pass merely because the assignments were spelled in some other way.
${CLICKHOUSE_CURL} -sS "${URL}/play" | grep -oF 'clearBar' | head -n1
${CLICKHOUSE_CURL} -sS "${URL}/play" | grep -oE "setProperty\('--progress', '0%'\)" | head -n1
