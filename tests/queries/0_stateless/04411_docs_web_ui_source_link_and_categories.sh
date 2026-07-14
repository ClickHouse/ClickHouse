#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` documentation search page reads the `source` column of `system.documentation`
# to show a pencil link to each entity's source file on GitHub, and color-codes every kind of entity
# the table exposes (including the more recently added compression codecs, profile events, metrics,
# and the system tables themselves). This guards that the served page is wired up for both.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The search query selects the `source` column.
echo "$PAGE" | grep -oF 'SELECT name, type, description, source' | head -n1

# The pencil link points at the source file on the `master` branch of the repository on GitHub.
echo "$PAGE" | grep -oF 'https://github.com/ClickHouse/ClickHouse/blob/master/' | head -n1

# Every additional kind of entity has an accent color, so they are all distinguishable in the list.
echo "$PAGE" | grep -oF "'Compression Codec'" | head -n1
echo "$PAGE" | grep -oF "'Profile Event'" | head -n1
echo "$PAGE" | grep -oF "'Current Metric'" | head -n1
echo "$PAGE" | grep -oF "'Asynchronous Metric'" | head -n1
echo "$PAGE" | grep -oF "'System Table'" | head -n1
