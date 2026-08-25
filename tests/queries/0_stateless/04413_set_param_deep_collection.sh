#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Stringifying a query parameter value (SET param_*) walks the parsed Field through a
# dedicated recursive visitor (ParameterFieldVisitorToString). With a raised max_parser_depth
# a deeply nested array/tuple/map value used to overflow the native stack while the server
# parsed the SET statement. It must report TOO_DEEP_RECURSION (code 306), never crash.
# Sent over HTTP because the client interprets SET param_* locally instead of sending it.
python3 -c "print('SET param_p = ' + '['*30000 + '1' + ']'*30000)" \
    | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&max_parser_depth=100000000&max_query_size=1000000000" --data-binary @- \
    | grep -oE "Code: [0-9]+" | head -1
