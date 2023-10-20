#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'SELECT 1 UNION DISTINCT SELECT 2 FORMAT PrettyCompactMonoBlock' | curl 'http://localhost:8123/' --data-binary @-
