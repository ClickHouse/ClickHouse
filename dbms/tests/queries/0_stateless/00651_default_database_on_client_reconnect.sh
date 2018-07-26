#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --ignore-error --multiquery --query "USE test; DROP TABLE IF EXISTS tab; CREATE TABLE tab (val UInt64) engine = Memory; SHOW CREATE TABLE tab format abcd;  DESC tab; DROP TABLE tab;" 2> /dev/null
