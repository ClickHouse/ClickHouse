#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

QUERY="CREATE USER user IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"

$CLICKHOUSE_FORMAT --hilite --show_secrets <<< "$QUERY"

$CLICKHOUSE_FORMAT --highlight --show_secrets <<< "$QUERY"

