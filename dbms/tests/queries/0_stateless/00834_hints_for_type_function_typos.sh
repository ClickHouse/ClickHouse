#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select c23ount(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['count'" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select cunt(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['count'" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select positin(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['position'" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select POSITIO(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['position'" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select fount(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['count'" | grep "Maybe you meant: \['round'" | grep "Or unknown aggregate function" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select positin(*) from system.functions;" 2>&1 | grep -v "Or unknown aggregate function" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select pov(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['pow','cos'\]" &>/dev/null;
