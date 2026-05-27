#!/usr/bin/env bash
# Tags: no-random-settings
# `--max_threads` is passed on the command line below; the random-settings
# injector would clobber that with its own value at connect time.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Verifies that command-line `--setting` overrides survive `RESET SESSION` on
# the client side. The client snapshots its effective settings at connect time
# and restores from that snapshot rather than from compiled-in defaults, so a
# client invoked with `--max_threads 7` keeps `max_threads = 7` after a reset.

${CLICKHOUSE_CLIENT_BINARY} ${CLICKHOUSE_CLIENT_OPT0} --max_threads 7 -m -q "
    SELECT 'at start:', getSetting('max_threads');
    SET max_threads = 1;
    SELECT 'after SET:', getSetting('max_threads');
    RESET SESSION;
    SELECT 'after RESET:', getSetting('max_threads');
"
