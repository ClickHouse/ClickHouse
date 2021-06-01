#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

psql --host localhost --port ${CLICKHOUSE_PORT_POSTGRESQL} default -c "SELECT NULL;"
