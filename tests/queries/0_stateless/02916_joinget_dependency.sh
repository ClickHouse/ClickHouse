#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We test the dependency on the DROP

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS Sub_distributed;
    DROP TABLE IF EXISTS Sub;
    DROP TABLE IF EXISTS Mapping;

    CREATE TABLE Mapping (Id UInt64, RegionId UInt64) ENGINE = Join(ANY,LEFT,Id);
    INSERT INTO Mapping VALUES (1,1);
    CREATE TABLE Sub (Id UInt64, PropertyId UInt64) ENGINE = MergeTree() PRIMARY KEY (Id) ORDER BY (Id);
    CREATE TABLE Sub_distributed (Id UInt64, PropertyId UInt64)ENGINE = Distributed('test_shard_localhost', $CLICKHOUSE_DATABASE, Sub, joinGet('$CLICKHOUSE_DATABASE.Mapping','RegionId',PropertyId));"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE Mapping;
" 2>&1 | grep -cm1 "HAVE_DEPENDENT_OBJECTS"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE Sub_distributed;
    DROP TABLE Sub;
    DROP TABLE Mapping;
"
