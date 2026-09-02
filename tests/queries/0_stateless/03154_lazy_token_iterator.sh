#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In previous versions this command took longer than ten minutes. Now it takes less than a second in release mode:

python3 -c 'import sys; import struct; sys.stdout.buffer.write(b"".join(struct.pack("<Q", 36) + b"\x40" + f"{i:064}".encode("ascii") for i in range(1024 * 1024)))' |
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&max_query_size=100000000&query=INSERT+INTO+FUNCTION+null('timestamp+UInt64,+label+String')+FORMAT+RowBinary" --data-binary @-
