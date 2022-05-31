#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for i in $(seq 1 1000);
do
    $CLICKHOUSE_CLIENT -q "SELECT * FROM url('https://datasets.clickhouse.com/github_events_v2.native.xz') LIMIT 1" &
done
