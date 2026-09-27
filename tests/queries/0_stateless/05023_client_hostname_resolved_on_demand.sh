#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client hostname is resolved on demand, so it must still be reported to whoever asks for it.
${CLICKHOUSE_LOCAL} --query "SELECT client_hostname != '' FROM system.processes"

# Over the native protocol the server keeps the name it received from the client.
${CLICKHOUSE_CLIENT} --query "SELECT client_hostname != '' FROM system.processes WHERE query_id = queryID()"
