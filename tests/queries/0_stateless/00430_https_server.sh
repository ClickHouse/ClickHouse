#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# TODO: enable this test with self-signed server cert

# ${CLICKHOUSE_CURL} -s --insecure 'https://localhost:8443/' -d 'SELECT number FROM system.numbers LIMIT 1'
