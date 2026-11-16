#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The `/dialect`, `/lang` and `/language` commands are interactive-only. A noninteractive
# `clickhouse-local` script accepts the other `/`-commands, so its typo diagnostics must neither
# accept the interactive-only ones nor suggest them, while the accepted ones are still suggested.

echo '-- an interactive-only command is rejected with an explicit message, not as SQL'
for command in '/dialect kusto' '/lang kusto' '/language' '/DIALECT clickhouse'; do
    $CLICKHOUSE_LOCAL -q "$command" 2>&1 | grep -o 'The `/[a-z]*` command is available in interactive mode only'
done

echo '-- a misspelled interactive-only command is not resolved to it'
$CLICKHOUSE_LOCAL -q '/dialec kusto' 2>&1 | grep -o 'Unknown command `/dialec`[^(]*'

echo '-- the commands accepted in a script are still suggested'
$CLICKHOUSE_LOCAL -q '/hepl' 2>&1 | grep -o 'Unknown command `/hepl`[^(]*'
$CLICKHOUSE_LOCAL -q '/clear foo' 2>&1 | grep -o 'The `/clear` command does not accept an argument'

echo '-- the diagnostics do not affect the SQL of a script'
$CLICKHOUSE_LOCAL -q 'SELECT 1 /* a comment */'
