#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

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
$CLICKHOUSE_CLIENT -q "select multisearchallposicionutf7('abc');" 2>&1 | grep "Maybe you meant: \['multiSearchAllPositionsUTF8','multiSearchAllPositions'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multisearchallposicionutf7casesensitive('abc');" 2>&1 | grep "Maybe you meant: \['multiSearchAllPositionsCaseInsensitive','multiSearchAllPositionsCaseInsensitiveUTF8'\]." &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multiSearchAllposicionutf7sensitive('abc');" 2>&1 | grep "Maybe you meant: \['multiSearchAllPositionsCaseInsensitive','multiSearchAnyCaseInsensitive'\]." &>/dev/null;
$CLICKHOUSE_CLIENT -q "select multiSearchAllPosicionSensitiveUTF8('abc');" 2>&1 | grep "Maybe you meant: \['multiSearchAnyCaseInsensitiveUTF8','multiSearchAllPositionsCaseInsensitiveUTF8'\]." &>/dev/null;

$CLICKHOUSE_CLIENT -q "select * FROM numberss(10);" 2>&1 | grep "Maybe you meant: \['numbers'\,'numbers_mt'\]." &>/dev/null
$CLICKHOUSE_CLIENT -q "select * FROM anothernumbers(10);" 2>&1 | grep -v "Maybe you meant: \['numbers'\,'numbers_mt'\]." &>/dev/null
$CLICKHOUSE_CLIENT -q "select * FROM mynumbers(10);" 2>&1 | grep "Maybe you meant: \['numbers'\]." &>/dev/null

$CLICKHOUSE_CLIENT -q "CREATE TABLE stored_aggregates (d Date, Uniq AggregateFunction(uniq, UInt64)) ENGINE = MergeTre(d, d, 8192);" 2>&1 | grep "Maybe you meant: \['MergeTree'\]." &>/dev/null
$CLICKHOUSE_CLIENT -q "CREATE TABLE stored_aggregates (d Date, Uniq AgregateFunction(uniq, UInt64)) ENGINE = MergeTree(d, d, 8192);" 2>&1 | grep "Maybe you meant: \['AggregateFunction'\]." &>/dev/null
