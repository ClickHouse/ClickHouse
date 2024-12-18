#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# head by default print 10 rows, but it is not enough to query 11 rows, since
# we need to overflow the default pipe size, hence just 1 million of rows (it
# should be around 6 MiB in text representation, should be definitelly enough).
$CLICKHOUSE_CLIENT --ignore-error -m --pager head -q "
    select * from numbers(1e6); -- { clientError CANNOT_WRITE_TO_FILE_DESCRIPTOR }
    select * from numbers(1e6); -- { clientError CANNOT_WRITE_TO_FILE_DESCRIPTOR }
"

exit 0
