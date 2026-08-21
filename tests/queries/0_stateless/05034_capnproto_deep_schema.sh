#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Cap'n Proto schema parser is recursive, so a deeply nested type expression used to exhaust
# the thread stack while the schema was being parsed.

DEPTH=20000
NESTED="$(python3 -c "print('List(' * $DEPTH + 'Int32' + ')' * $DEPTH)")"

${CLICKHOUSE_LOCAL} --logger.console=0 --query "
DESC format(CapnProto, '')
SETTINGS
    format_schema_source = 'string',
    format_schema = '@0x844f048b15c12dab;\nstruct M { data @0 :${NESTED}; }',
    format_schema_message_name = 'M'
" 2>&1 | grep -c -F 'nested too deeply'

${CLICKHOUSE_LOCAL} --logger.console=0 --query "
DESC format(CapnProto, '')
SETTINGS
    format_schema_source = 'string',
    format_schema = '@0x844f048b15c12dab;\nstruct M { data @0 :List(List(Int32)); }',
    format_schema_message_name = 'M'
"
