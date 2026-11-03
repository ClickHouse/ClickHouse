#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly test_user=test_user_03732_${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $test_user;
    CREATE USER $test_user;
    GRANT SELECT ON * to $test_user;
    DROP USER IF EXISTS $test_user;
"

