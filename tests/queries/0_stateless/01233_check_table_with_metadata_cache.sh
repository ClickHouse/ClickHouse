#!/usr/bin/env bash
# Tags: no-fasttest, long, no-s3-storage
# Tag no-fasttest: setting use_metadata_cache=true is not supported in fasttest, because clickhouse binary in fasttest is build without RocksDB.
# To suppress Warning messages from CHECK TABLE
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

table_engines=(ReplicatedMergeTree)
database_engines=(Ordinary Atomic)
use_metadata_caches=(false true)
use_projections=(false true)
use_compact_data_parts=(false true)

for table_engine in "${table_engines[@]}"; do
    for database_engine in "${database_engines[@]}"; do
        for use_metadata_cache in "${use_metadata_caches[@]}"; do
            for use_projection in "${use_projections[@]}"; do
                for use_compact_data_part in "${use_compact_data_parts[@]}"; do
                    echo "database engine:${database_engine}; table engine:${table_engine}; use metadata cache:${use_metadata_cache}; use projection:${use_projection}; use_compact_data_part:${use_compact_data_part}"

                    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_metadata_cache.check_part_metadata_cache SYNC;"
                    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS test_metadata_cache;"
                    ${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --query "CREATE DATABASE test_metadata_cache ENGINE = ${database_engine};"

                    table_engine_clause=""
                    if [[ "$table_engine" == "ReplicatedMergeTree" ]]; then
                        table_engine_clause="ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_metadata_cache/check_part_metadata_cache', 'r1')"
                    elif [[ "$table_engine" == "MergeTree" ]]; then
                        table_engine_clause="ENGINE MergeTree()"
                    fi

                    projection_clause=""
                    if [[ "$use_projection" == "true" ]]; then
                        projection_clause=", projection p1 (select p, sum(k), sum(v1), sum(v2) group by p)"
                    fi

                    compact_data_part_clause=", min_bytes_for_wide_part = 10485760"
                    if [[ $use_compact_data_part == "true" ]]; then
                        compact_data_part_clause=", min_bytes_for_wide_part = 0"
                    fi
                    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_metadata_cache.check_part_metadata_cache (p Date, k UInt64, v1 UInt64, v2 Int64${projection_clause}) $table_engine_clause PARTITION BY toYYYYMM(p) ORDER BY k settings use_metadata_cache = ${use_metadata_cache} ${compact_data_part_clause}"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Insert first batch of data.
                    ${CLICKHOUSE_CLIENT} --echo --query "INSERT INTO test_metadata_cache.check_part_metadata_cache (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Insert second batch of data.
                    ${CLICKHOUSE_CLIENT} --echo --query "INSERT INTO test_metadata_cache.check_part_metadata_cache (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-18', 8, 7000, 8000);"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # First update.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache update  v1 = 2001  where k = 1 settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Second update.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache update  v2 = 4002  where k = 1 settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # First delete.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache delete where k = 1 settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Second delete.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache delete where k = 8 settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Insert third batch of data.
                    ${CLICKHOUSE_CLIENT} --echo --query "INSERT INTO test_metadata_cache.check_part_metadata_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Drop one partition.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache drop partition 201805 settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Add column.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache add column v3 UInt64 settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Delete column.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache drop column v3 settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Add TTL.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache modify TTL p + INTERVAL 10 YEAR settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Modify TTL.
                    ${CLICKHOUSE_CLIENT} --echo --query "ALTER TABLE test_metadata_cache.check_part_metadata_cache modify TTL p + INTERVAL 15 YEAR settings mutations_sync = 1, replication_alter_partitions_sync = 1;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"

                    # Truncate table.
                    ${CLICKHOUSE_CLIENT} --echo --query "TRUNCATE TABLE test_metadata_cache.check_part_metadata_cache;"
                    ${CLICKHOUSE_CLIENT} --echo --query "CHECK TABLE test_metadata_cache.check_part_metadata_cache;"
                done
            done
        done
    done
done
