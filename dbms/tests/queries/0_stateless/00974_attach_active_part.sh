#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS attach_bug";
$CLICKHOUSE_CLIENT --query="CREATE TABLE attach_bug (n UInt64) ENGINE = MergeTree() PARTITION BY intDiv(n, 4) ORDER BY n";
$CLICKHOUSE_CLIENT --query="INSERT INTO attach_bug SELECT number FROM system.numbers LIMIT 16";
$CLICKHOUSE_CLIENT --query="ALTER TABLE attach_bug ATTACH PART '../1_2_2_0'" 2> /dev/null; # | grep "<some exception message>"
$CLICKHOUSE_CLIENT --query="SElECT name FROM system.parts WHERE table='attach_bug' ORDER BY name FORMAT TSV";
$CLICKHOUSE_CLIENT --query="SElECT count() FROM attach_bug FORMAT TSV";             # will fail
$CLICKHOUSE_CLIENT --query="DROP TABLE attach_bug";


