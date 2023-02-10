#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS orc";

$CLICKHOUSE_CLIENT --query="CREATE TABLE orc (array1 Array(Int32), array2 Array(Array(Int32))) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO orc VALUES ([1,2,3,4,5], [[1,2], [3,4], [5]]), ([42], [[42, 42], [42]])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM orc FORMAT ORC";

$CLICKHOUSE_CLIENT --query="DROP TABLE orc";

