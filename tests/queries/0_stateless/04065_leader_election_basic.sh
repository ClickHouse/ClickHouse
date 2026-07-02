#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Test basic leader election functionality on S3 storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_leader_election_s3"

# Create a table with leader_election enabled on a shared-metadata S3 disk
# (`plain_rewritable` — the only metadata layout where parts written by one
# node are visible to another). The instance should become leader since it's
# the only writer.
#
# Use an inline disk definition with a per-database endpoint instead of the
# shared `s3_plain_rewritable` disk from `tests/config/config.d/storage_conf.xml`.
# The shared disk uses one bucket prefix for all concurrent tests, and its
# `MetadataStorageFromPlainRewritableObjectStorage` cache is shared across
# tables on the same disk handle — concurrent activity from unrelated tests
# can briefly desync the cache view of a freshly inserted part, surfacing as
# `FILE_DOESNT_EXIST` for `data.bin` on the very next read. A per-database
# endpoint isolates this test's bucket prefix and metadata cache.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE test_leader_election_s3 (x UInt64, s String)
    ENGINE = MergeTree ORDER BY x
    SETTINGS
        disk = disk(
            name = '04065_le_${CLICKHOUSE_DATABASE}',
            type = s3_plain_rewritable,
            endpoint = 'http://localhost:11111/test/04065_le_${CLICKHOUSE_DATABASE}/',
            access_key_id = clickhouse,
            secret_access_key = clickhouse),
        leader_election = true,
        leader_election_heartbeat_interval = 1, leader_election_session_timeout = 5
"

# Wait until the leader election heartbeat completes and the instance becomes leader.
# Retry the INSERT instead of using a fixed sleep — fixed sleeps are timing-dependent
# and can flake on slow or overloaded CI workers.
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
    if $CLICKHOUSE_CLIENT -q "INSERT INTO test_leader_election_s3 SELECT number, toString(number) FROM numbers(100)" 2>/dev/null; then
        break
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_leader_election_s3"

# Verify data correctness.
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM test_leader_election_s3"

# Note: mutations are not exercised here because `s3_plain_rewritable` (the only
# shared-metadata disk configured in stateless tests, and the only one this test
# can use under the `leader_election` storage-policy validation) does not support
# hard links and therefore rejects `ALTER ... DELETE` regardless of leadership.
# Leader-only mutation acceptance is covered in the integration test instead, by
# rejecting writes on the follower (the failure mode that mutations would expose).

$CLICKHOUSE_CLIENT -q "DROP TABLE test_leader_election_s3"
