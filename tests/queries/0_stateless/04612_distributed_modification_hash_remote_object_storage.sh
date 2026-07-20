#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# Tests that the modification_hash of a Distributed table fails closed when the underlying table on a
# *remote* shard is one whose hash cannot be validated from a separate probe query (issue #108713).
#
# The remote-shard probe in StorageDistributed::getModificationHash is a separate system.tables query on
# the shard, so it can never see the reading query's consumed-object-set capture
# (QueryConsumedObjectSets). For a listing-based object-storage child the shard would silently fall back
# to a fresh listing, reopening the listing A -> B -> A membership race the capture closes: the pre-probe
# lists {a}, the shard read consumes {a, b}, b disappears, and the post-probe lists {a} again, so the
# initiator could keep a stale query-cache entry or a stale REFRESH ... IF CHANGED source hash. Wrapper
# engines (Merge, Distributed) can reach such tables transitively on the shard, out of the initiator's
# sight. The probe must therefore only accept engines whose hash is probe-consistent (the MergeTree
# family, Memory, Log, StripeLog, URL) and fail closed for everything else.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Object names are unique per run so concurrent runs (e.g. the flaky check) do not collide on the
# shared S3 bucket.
prefix="test_04612_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${prefix}_1', format = 'TSV', structure = 'x UInt64') SELECT 10 SETTINGS s3_truncate_on_insert = 1"

# test_cluster_one_shard_remote is a single shard whose only replica is 127.0.0.2, which is never treated
# as a local address (see isLocalAddress.cpp), so the probe always takes the remote system.tables path.
# All tables live in the test's own (Atomic) database, so every table has a UUID and none of the
# missing-UUID fail-closed rules interfere.

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_mt (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_mt VALUES (1)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_s3 (x UInt64) ENGINE = S3(s3_conn, filename = '${prefix}_*', format = 'TSV')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^t_mt$')"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE dist_over_mt (x UInt64) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), 't_mt')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dist_over_s3 (x UInt64) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), 't_s3')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dist_over_merge (x UInt64) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), 't_merge')"

# A probe-consistent engine (MergeTree) on the remote shard keeps reporting a hash.
${CLICKHOUSE_CLIENT} -q "SELECT 'remote MergeTree hash not null', modification_hash IS NOT NULL FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_mt'"

# A listing-based object-storage engine on the remote shard fails closed: its hash is only sound when
# validated against the reading query's consumed object set, which a separate probe can never see.
${CLICKHOUSE_CLIENT} -q "SELECT 'remote S3 hash null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_s3'"

# A wrapper engine on the remote shard fails closed too: it can reach such tables transitively.
${CLICKHOUSE_CLIENT} -q "SELECT 'remote Merge hash null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_merge'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE dist_over_mt, dist_over_s3, dist_over_merge, t_merge, t_s3, t_mt"
