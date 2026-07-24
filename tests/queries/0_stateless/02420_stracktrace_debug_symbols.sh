#!/usr/bin/env bash
# shellcheck disable=SC2015

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE: that this test uses stacktrace instead of addressToLineWithInlines() or
# similar, since that code (use / might use) different code path in Dwarf
# parser.
#
# Also note, that to rely on this test one should assume that CI packages uses
# ThinLTO builds.
#
# Due to inlining, it can show vector instead of Exception.cpp

$CLICKHOUSE_LOCAL --stacktrace -q 'select throwIf(1)' |& grep -q -P '(Common/Exception.cpp|libcxx/include/vector):[0-9]*: DB::Exception::Exception' && echo 1 || $CLICKHOUSE_LOCAL --stacktrace -q 'select throwIf(1)'
