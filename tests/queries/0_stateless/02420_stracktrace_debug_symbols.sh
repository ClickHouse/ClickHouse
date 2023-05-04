#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE: that this test uses stacktrace instead of addressToLineWithInlines() or
# similar, since that code (use / might use) different code path in Dwarf
# parser.
#
# Also note, that to rely on this test one should assume that CI packages uses
# ThinLTO builds.

$CLICKHOUSE_LOCAL --stacktrace -q 'select throwIf(1)' |& grep -c 'Common/Exception.cpp:[0-9]*: DB::Exception::Exception'
