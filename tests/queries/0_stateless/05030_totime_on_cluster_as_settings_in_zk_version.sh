#!/usr/bin/env bash
# `CREATE TABLE ... AS` ON CLUSTER materializes the source definition on the worker, so the initiator
# cannot rewrite the legacy `toTime` spelling for it. Only `distributed_ddl_entry_format_version = 1`
# drops the query settings, so only that version has to be rejected: version 2 carries
# `use_legacy_to_time` to the worker, which then stores exactly what a local `CREATE` stores.
# `CLONE AS` stays rejected, because the worker-side rewrite skips clones on purpose.
# Names are database-qualified: the pre-`NORMALIZE_CREATE_ON_INITIATOR_VERSION` entry ships the query
# text as written, so a worker resolves unqualified names in its own default database.
# The source is created with `use_legacy_to_time = 0`, so its stored key is the raw `toTime` spelling
# and a verbatim copy is distinguishable from a rewritten one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 0 -q "
CREATE TABLE src (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
SELECT 'source', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'src';
"

LEGACY=(--allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --distributed_ddl_output_mode none)

# The oldest entry format carries no settings at all, so both forms stay rejected.
${CLICKHOUSE_CLIENT} "${LEGACY[@]}" --distributed_ddl_entry_format_version 1 -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_v1 ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.src;
" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED' | sed 's/^/oldest_as /'

${CLICKHOUSE_CLIENT} "${LEGACY[@]}" --distributed_ddl_entry_format_version 1 -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_clone_v1 ON CLUSTER test_shard_localhost CLONE AS ${CLICKHOUSE_DATABASE}.src;
" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED' | sed 's/^/oldest_clone /'

# A clone must keep its source definition verbatim, so it cannot be made unambiguous by the setting.
${CLICKHOUSE_CLIENT} "${LEGACY[@]}" --distributed_ddl_entry_format_version 2 -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_clone_v2 ON CLUSTER test_shard_localhost CLONE AS ${CLICKHOUSE_DATABASE}.src;
" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED' | sed 's/^/settings_in_zk_clone /'

# The same query without ON CLUSTER: this is the spelling a worker has to end up with too, asserted
# for `ON CLUSTER` in `05030_totime_on_cluster_as_settings_in_zk_worker.sh`.
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 -q "
CREATE TABLE dst_local AS src;
SELECT 'local_as', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'dst_local';
"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE dst_local;
DROP TABLE src;
"
