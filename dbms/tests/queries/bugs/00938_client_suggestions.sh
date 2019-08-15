#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

for i in {1..100}; do $CLICKHOUSE_CLIENT --always_load_suggestion_data --query="SELECT 1 FORMAT Null"; done
