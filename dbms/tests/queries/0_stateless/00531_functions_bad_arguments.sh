#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Server should not crash on any function trash calls

cat $CURDIR/00531_filimonov.data | $CLICKHOUSE_CLIENT -n --ignore-error >/dev/null 2>&1

# todo: maybe add more strange usages
perl -E "say \$_ for map {chomp; (qq{SELECT \$_;}, qq{SELECT \$_();}, qq{SELECT \$_(NULL);}, qq{SELECT \$_([]);}, qq{SELECT \$_([NULL]);}, qq{SELECT \$_(-1);}, qq{SELECT \$_('');},)} qx{$CLICKHOUSE_CLIENT -q 'SELECT name FROM system.functions ORDER BY name;'}" | $CLICKHOUSE_CLIENT -n --ignore-error >/dev/null 2>&1

$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'"
