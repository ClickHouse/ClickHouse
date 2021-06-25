#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_FORMAT --hilite <<< "CREATE USER user IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"
