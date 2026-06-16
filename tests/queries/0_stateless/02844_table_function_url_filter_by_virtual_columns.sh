#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --print-profile-events -q "select * from url('http://localhost:11111/test/{a,b,c}.tsv', auto, 'x UInt64, y UInt64, z UInt64') where _file = 'a.tsv' format Null" 2>&1 | grep -F -c "EngineFileLikeReadFiles: 1"

$CLICKHOUSE_LOCAL --print-profile-events -q "select * from url('http://localhost:11111/test/{a,b,c}.tsv', auto, 'x UInt64, y UInt64, z UInt64') where _path = '/test/a.tsv' format Null" 2>&1 | grep -F -c "EngineFileLikeReadFiles: 1"

