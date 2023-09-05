#!/usr/bin/env bash
# Tags: no-s3-storage, no-asan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e
set -o pipefail

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted
verify_sql="SELECT
    (SELECT sumIf(value, metric = 'PartsCompact'), sumIf(value, metric = 'PartsWide') FROM system.metrics) =
    (SELECT countIf(part_type = 'Compact'), countIf(part_type = 'Wide')
        FROM (SELECT part_type FROM system.parts UNION ALL SELECT part_type FROM system.projection_parts))"

# The query is not atomic - it can compare states between system.parts and system.metrics from different points in time.
# So, there is inherent race condition (especially in fasttest that runs tests in parallel).
#
# But it should get the expected result eventually.
# In case of test failure, this code will do infinite loop and timeout.
verify()
{
    while true; do
        result=$( $CLICKHOUSE_CLIENT -m --query="$verify_sql" )
        if [ "$result" = "1" ]; then
            echo 1
            return
        fi
        sleep 0.1
    done
}

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=1 --query="DROP TABLE IF EXISTS data_01600"
# Compact  - (5..10]
# Wide     - >10
$CLICKHOUSE_CLIENT --query="CREATE TABLE data_01600 (part_type String, key Int) ENGINE = MergeTree PARTITION BY part_type ORDER BY key SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=10, index_granularity = 8192, index_granularity_bytes = '10Mi'"

# Compact
$CLICKHOUSE_CLIENT --query="INSERT INTO data_01600 SELECT 'Compact', number FROM system.numbers LIMIT 6"
verify

# Wide
$CLICKHOUSE_CLIENT --query="INSERT INTO data_01600 SELECT 'Wide', number FROM system.numbers LIMIT 11 OFFSET 6"
verify

# DROP and check
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=1 --query="DROP TABLE data_01600"
verify
