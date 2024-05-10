#!/usr/bin/env bash
# Tags: replica
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS wrong_metadata"

$CLICKHOUSE_CLIENT -n --query="CREATE TABLE wrong_metadata(
    a UInt64,
    b UInt64,
    c UInt64
)
ENGINE ReplicatedMergeTree('/test/{database}/tables/wrong_metadata', '1')
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0"

$CLICKHOUSE_CLIENT --query="INSERT INTO wrong_metadata VALUES (1, 2, 3)"


$CLICKHOUSE_CLIENT --query="SYSTEM STOP MERGES wrong_metadata"


$CLICKHOUSE_CLIENT --query="ALTER TABLE wrong_metadata RENAME COLUMN a TO a1, RENAME COLUMN b to b1 SETTINGS replication_alter_partitions_sync = 0"

counter=0 retries=60
I=0
while [[ $counter -lt $retries ]]; do
    I=$((I + 1))
    result=$($CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE wrong_metadata")
    if [[ $result == *"\`a1\` UInt64"* ]]; then
        break;
    fi
    sleep 0.1
    ((++counter))
done


$CLICKHOUSE_CLIENT --query="SELECT * FROM wrong_metadata ORDER BY a1 FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT --query="SELECT '~~~~~~~'"

$CLICKHOUSE_CLIENT --query="INSERT INTO wrong_metadata VALUES (4, 5, 6)"


$CLICKHOUSE_CLIENT --query="SELECT * FROM wrong_metadata ORDER BY a1 FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="SELECT '~~~~~~~'"


$CLICKHOUSE_CLIENT --query="ALTER TABLE wrong_metadata RENAME COLUMN a1 TO b, RENAME COLUMN b1 to a SETTINGS replication_alter_partitions_sync = 0"

counter=0 retries=60
I=0
while [[ $counter -lt $retries ]]; do
    I=$((I + 1))
    result=$($CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE table = 'wrong_metadata' AND database='${CLICKHOUSE_DATABASE}'")
    if [[ $result == *"b1 TO a"* ]]; then
        break;
    fi
    sleep 0.1
    ((++counter))
done

$CLICKHOUSE_CLIENT --query="INSERT INTO wrong_metadata VALUES (7, 8, 9)"

$CLICKHOUSE_CLIENT --query="SELECT * FROM wrong_metadata ORDER by a1 FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="SELECT '~~~~~~~'"

$CLICKHOUSE_CLIENT --query="SYSTEM START MERGES wrong_metadata"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA wrong_metadata"

$CLICKHOUSE_CLIENT --query="SELECT * FROM wrong_metadata order by a FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT --query="SELECT '~~~~~~~'"


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS wrong_metadata"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS wrong_metadata_compact"

$CLICKHOUSE_CLIENT -n --query="CREATE TABLE wrong_metadata_compact(
    a UInt64,
    b UInt64,
    c UInt64
)
ENGINE ReplicatedMergeTree('/test/{database}/tables/wrong_metadata_compact', '1')
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 10000000"

$CLICKHOUSE_CLIENT --query="INSERT INTO wrong_metadata_compact VALUES (1, 2, 3)"

$CLICKHOUSE_CLIENT --query="SYSTEM STOP MERGES wrong_metadata_compact"

$CLICKHOUSE_CLIENT --query="ALTER TABLE wrong_metadata_compact RENAME COLUMN a TO a1, RENAME COLUMN b to b1 SETTINGS replication_alter_partitions_sync = 0"

counter=0 retries=60
I=0
while [[ $counter -lt $retries ]]; do
    I=$((I + 1))
    result=$($CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE wrong_metadata_compact")
    if [[ $result == *"\`a1\` UInt64"* ]]; then
        break;
    fi
    sleep 0.1
    ((++counter))
done

$CLICKHOUSE_CLIENT --query="SELECT * FROM wrong_metadata_compact ORDER BY a1 FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="SELECT '~~~~~~~'"

$CLICKHOUSE_CLIENT --query="INSERT INTO wrong_metadata_compact VALUES (4, 5, 6)"

$CLICKHOUSE_CLIENT --query="SELECT * FROM wrong_metadata_compact ORDER BY a1 FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="SELECT '~~~~~~~'"

$CLICKHOUSE_CLIENT --query="ALTER TABLE wrong_metadata_compact RENAME COLUMN a1 TO b, RENAME COLUMN b1 to a SETTINGS replication_alter_partitions_sync = 0"

counter=0 retries=60
I=0
while [[ $counter -lt $retries ]]; do
    I=$((I + 1))
    result=$($CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE table = 'wrong_metadata_compact' AND database='${CLICKHOUSE_DATABASE}'")
    if [[ $result == *"b1 TO a"* ]]; then
        break;
    fi
    sleep 0.1
    ((++counter))
done

$CLICKHOUSE_CLIENT --query="INSERT INTO wrong_metadata_compact VALUES (7, 8, 9)"

$CLICKHOUSE_CLIENT --query="SELECT * FROM wrong_metadata_compact ORDER by a1 FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="SELECT '~~~~~~~'"

$CLICKHOUSE_CLIENT --query="SYSTEM START MERGES wrong_metadata_compact"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA wrong_metadata_compact"

$CLICKHOUSE_CLIENT --query="SELECT * FROM wrong_metadata_compact order by a FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="SELECT '~~~~~~~'"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS wrong_metadata_compact"
