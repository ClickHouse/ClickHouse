#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We need to see exceptions from both queries
echo "SELECT 1 LIMIT 1 WITH TIES BY 1; SELECT 1 FROM idontexist;" |
    $CLICKHOUSE_CLIENT --enable-analyzer 1 --ignore-error 2>&1 |
    grep -Fao -e 'Can not use WITH TIES alongside LIMIT BY' -e 'Unknown table expression identifier' |
# We will see 'Can not use WITH TIES alongside LIMIT BY' only once, because this is a client error.
# and 'Unknown table expression identifier' one or two times, depends whether the log line from server
# gets propagated back to the client. So 2 or 3 in total.
    wc -l | grep -q -e "2" -e "3" && echo "Match"
