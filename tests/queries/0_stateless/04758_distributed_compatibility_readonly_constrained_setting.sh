#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `compatibility` reverts `network_compression_method` / `network_zstd_compression_level` to their old
# defaults. The interserver senders (`MultiplexedConnections`, `HedgedConnections`) and distributed
# INSERT (`RemoteInserter`) send `compatibility` itself and let the remote server re-derive those
# values, exactly as the native client does for the initial query (see 04612 for the client-path
# coverage).
#
# This test covers the end-to-end path: a distributed query under an old `compatibility` runs against a
# shard whose profile pins the new defaults read-only, and an explicit override is still rejected. It
# does not, on its own, prove the sender-side demotion: `TCPHandler` clamps a secondary query's setting
# violations instead of throwing (`clampToSettingsConstraints`), so serializing the derived values would
# not fail here - it would silently make the shard run under its pinned value instead of the one
# `compatibility` selects. The demotion itself is pinned by `gtest_secondary_query_settings`.

user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${profile}"

# A profile that pins the new network compression defaults read-only.
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${profile} SETTINGS network_compression_method = 'ZSTD' CONST, network_zstd_compression_level = 3 CONST"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT REMOTE, CREATE TEMPORARY TABLE ON *.* TO ${user}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04758 (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04758 VALUES (1)"

# Distributed SELECT under an old `compatibility` must not fail on the remote shard's pinned settings.
# Cover both interserver senders: hedged and non-hedged connections.
echo -n 'distributed_select_hedged: '
${CLICKHOUSE_CLIENT} --user "${user}" --compatibility 25.8 --use_hedged_requests 1 --query "SELECT count() FROM remote('127.0.0.{1,2}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_04758', '${user}')"
echo -n 'distributed_select_multiplexed: '
${CLICKHOUSE_CLIENT} --user "${user}" --compatibility 25.8 --use_hedged_requests 0 --query "SELECT count() FROM remote('127.0.0.{1,2}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_04758', '${user}')"

# Distributed INSERT (RemoteInserter) under an old `compatibility` must not fail either.
echo -n 'distributed_insert: '
${CLICKHOUSE_CLIENT} --user "${user}" --compatibility 25.8 --query "INSERT INTO FUNCTION remote('127.0.0.2:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_04758', '${user}') VALUES (2)" && echo OK
echo -n 'count_after_insert: '
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04758"

# A genuine explicit override is still rejected by the pin.
echo -n 'explicit_override: '
${CLICKHOUSE_CLIENT} --user "${user}" --network_compression_method LZ4 --query "SELECT count() FROM remote('127.0.0.{1,2}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_04758', '${user}')" 2>&1 | grep -o -m1 "SETTING_CONSTRAINT_VIOLATION"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04758"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${profile}"
