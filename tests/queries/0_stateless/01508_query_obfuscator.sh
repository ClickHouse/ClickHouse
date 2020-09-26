#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --seed Hello --obfuscate <<< "SELECT 123, 'Test://2020-01-01hello1234 at 2000-01-01T01:02:03', 12e100, Gibberish_id_testCool, hello(World), avgIf(remote('127.0.0.1'))"
