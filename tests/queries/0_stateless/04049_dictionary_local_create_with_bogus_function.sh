#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Verify that clickhouse-local doesn't crash when CREATE DICTIONARY
# contains a non-existing function in a list value (e.g., Decimal(18, 8)
# parsed as a function call). Should produce INCORRECT_DICTIONARY_DEFINITION.
$CLICKHOUSE_LOCAL -q "
CREATE DICTIONARY default.currency_conversion_dict
(
    \`a\` String,
    \`b\` Decimal(18, 8)
)
PRIMARY KEY a
SOURCE(CLICKHOUSE(
    TABLE ''
    STRUCTURE (
        a String
        b Decimal(18, 8)
    )
))
LIFETIME (MIN 0 MAX 3600)
LAYOUT (FLAT());
" 2>&1 | grep -o 'INCORRECT_DICTIONARY_DEFINITION'
