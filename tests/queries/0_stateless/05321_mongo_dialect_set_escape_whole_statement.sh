#!/usr/bin/env bash
# The `SET` escape of the `mongo` dialect applies only when the `SET` is the whole statement. A
# valid `SET` prefix followed by more text must not be executed as just the `SET`, silently
# dropping the rest. The queries go through HTTP, so that the server parses them rather than the
# client, which splits statements on its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_URL}&dialect=mongo&allow_experimental_mongo_dialect=1"

# A whole `SET`, with or without a trailing `;`, is accepted.
${CLICKHOUSE_CURL} -sS "$URL" -d "SET max_threads = 1" && echo "set: ok"
${CLICKHOUSE_CURL} -sS "$URL" -d "SET max_threads = 1;" && echo "set with terminator: ok"

# Trailing text after the `SET` is an error rather than being dropped.
${CLICKHOUSE_CURL} -sS "$URL" -d "SET max_threads = 1 garbage" | grep -c -m1 '^Code: '
${CLICKHOUSE_CURL} -sS "$URL" -d "SET dialect = 'clickhouse'; db.t.find({})" | grep -c -m1 '^Code: '
