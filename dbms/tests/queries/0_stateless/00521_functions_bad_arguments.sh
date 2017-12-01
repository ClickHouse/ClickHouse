#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Server should not crash on any function trash calls

# todo: make --ignore-error function in clickhouse-client and load all queries as one .sql (or pipe)
# todo: maybe add more strange usages
perl -E "chomp, say qx{$CLICKHOUSE_CLIENT -q '\$_'} for map {chomp; (qq{SELECT \$_;}, qq{SELECT \$_();}, qq{SELECT \$_(NULL);}, qq{SELECT \$_([]);}, qq{SELECT \$_([NULL]);})} qx{$CLICKHOUSE_CLIENT -q 'SELECT name FROM system.functions ORDER BY name;'}" 2>&1 >/dev/null

$CLICKHOUSE_CLIENT -q "Select 'Still alive'"
