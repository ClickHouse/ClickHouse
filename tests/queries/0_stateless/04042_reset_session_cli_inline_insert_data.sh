#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `RESET SESSION` is supposed to restore the client-side state to what the
# user passed on the command line, including settings that `Client::main`
# applies after `processOptions` already constructed `client_context`.
# `--inline-insert-data` is one such setting: `Client::processConfig`
# (called from `Client::main`) sets
# `send_table_structure_on_insert_with_inline_data = false` on
# `client_context`. Before this fix, the connection baseline snapshot was
# taken too early — inside `initClientContext`, which `Client::processOptions`
# runs *before* `processConfig` — so `RESET SESSION` would restore the
# server default (`true`) and the next query would behave as if
# `--inline-insert-data` had never been on the command line.

# Single multi-statement `clickhouse-client` invocation = one TCP session.
# Without `--inline-insert-data` the setting is true on every line; with it,
# `processConfig` flips it to false at startup and the post-reset assertion
# pins that the snapshot captured the flip.
${CLICKHOUSE_CLIENT} --inline-insert-data -m -q "
    SELECT '-- pre-reset (cli flag flipped to 0) --';
    SELECT getSetting('send_table_structure_on_insert_with_inline_data');

    SET send_table_structure_on_insert_with_inline_data = 1;
    SELECT '-- after SET (overridden back to 1) --';
    SELECT getSetting('send_table_structure_on_insert_with_inline_data');

    RESET SESSION;

    SELECT '-- post-reset (snapshot must include the cli flip; should be 0) --';
    SELECT getSetting('send_table_structure_on_insert_with_inline_data');
"
