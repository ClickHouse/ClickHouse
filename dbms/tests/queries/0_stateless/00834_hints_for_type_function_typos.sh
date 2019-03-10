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
$CLICKHOUSE_CLIENT -q "select pov(*) from system.functions;" 2>&1 | grep "Maybe you meant: \['pow'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select getColumnStructure('abc');" 2>&1 | grep "Maybe you meant: \['dumpColumnStructure'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select gutColumnStructure('abc');" 2>&1 | grep "Maybe you meant: \['dumpColumnStructure'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select gupColumnStructure('abc');" 2>&1 | grep "Maybe you meant: \['dumpColumnStructure'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select provideColumnStructure('abc');" 2>&1 | grep "Maybe you meant: \['dumpColumnStructure'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multiposicionutf7('abc');" 2>&1 | grep "Maybe you meant: \['multiPositionUTF8','multiPosition'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multiposicionutf7casesensitive('abc');" 2>&1 | grep "Maybe you meant: \['multiPositionCaseInsensitive'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multiposicionutf7sensitive('abc');" 2>&1 | grep "Maybe you meant: \['multiPositionCaseInsensitive'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multiPosicionSensitiveUTF8('abc');" 2>&1 | grep "Maybe you meant: \['multiPositionCaseInsensitiveUTF8'\]" &>/dev/null;
