#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Holds only the granular privilege of the command it runs.
query_cache_user="query_cache_user_04836_$CLICKHOUSE_DATABASE"
sync_user="sync_user_04836_$CLICKHOUSE_DATABASE"
# Holds the cache privilege group, which the ON CLUSTER path wrongly required for every cache
# command, including SYSTEM SYNC FILESYSTEM CACHE, which is not one of its children.
group_user="group_user_04836_$CLICKHOUSE_DATABASE"
# Negative control: CLUSTER only.
no_cache_user="no_cache_user_04836_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $query_cache_user, $sync_user, $group_user, $no_cache_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $query_cache_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, SYSTEM CLEAR QUERY CACHE ON *.* TO $query_cache_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $sync_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, SYSTEM SYNC FILESYSTEM CACHE ON *.* TO $sync_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $group_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, DROP CACHE ON *.* TO $group_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $no_cache_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $no_cache_user"

cluster="test_shard_localhost"
run() { ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none "$@"; }

# Scope the two commands that would otherwise touch server-global state shared with other tests:
# a tag restricts the query cache clear to entries of this run, and an unknown cache name makes the
# filesystem cache sync stop at BAD_ARGUMENTS on the host. Neither is read while computing the
# required access, so the assertions below are unaffected.
tag="04836_$CLICKHOUSE_DATABASE"
missing_cache="04836_no_such_cache_$CLICKHOUSE_DATABASE"
missing_disk="04836_no_such_disk_$CLICKHOUSE_DATABASE"

# The access check runs on the initiator before the task is enqueued, so a command that is denied
# never reaches a host. A command that is allowed reports the host error instead.
sync_allowed() {
    local out
    out=$(run --user "$1" --query "SYSTEM SYNC FILESYSTEM CACHE '$missing_cache' ON CLUSTER $cluster" 2>&1)
    if grep -qF ACCESS_DENIED <<< "$out"; then
        echo "FAIL: access denied: $out"
    elif grep -qF "There is no cache by name: $missing_cache" <<< "$out"; then
        echo "ok"
    else
        echo "FAIL: expected the host to reject the unknown cache name: $out"
    fi
}

# The ON CLUSTER path must require the same privilege as the local spelling, not the cache
# privilege group. Requiring the group demanded all of its children at once.

# A holder of only the query cache privilege is allowed (before: denied, missing 27 others).
run --user "$query_cache_user" --query "SYSTEM CLEAR QUERY CACHE TAG '$tag' ON CLUSTER $cluster" >/dev/null || exit 1
echo "ok"

# A holder of only SYSTEM SYNC FILESYSTEM CACHE is allowed.
sync_allowed "$sync_user"

# SYSTEM SYNC FILESYSTEM CACHE is parented to SYSTEM, not to the cache group, so a holder of
# the group never held it. Before the fix the group satisfied the ON CLUSTER check and the
# command ran on every host.
run --user "$group_user" --query "SYSTEM SYNC FILESYSTEM CACHE '$missing_cache' ON CLUSTER $cluster -- { serverError ACCESS_DENIED }"

# The other direction: the group still covers the commands it is the parent of, so an existing
# group holder keeps them. Green both before and after the fix.
run --user "$group_user" --query "SYSTEM CLEAR QUERY CACHE TAG '$tag' ON CLUSTER $cluster" >/dev/null || exit 1
echo "ok"

# Negative control: CLUSTER alone is not enough for either command.
run --user "$no_cache_user" --query "SYSTEM CLEAR QUERY CACHE TAG '$tag' ON CLUSTER $cluster -- { serverError ACCESS_DENIED }"
run --user "$no_cache_user" --query "SYSTEM SYNC FILESYSTEM CACHE '$missing_cache' ON CLUSTER $cluster -- { serverError ACCESS_DENIED }"

# Every command in the block, not just the two above. A user holding CLUSTER alone is denied on the
# initiator, and the message names the privilege that was required, so the whole mapping is asserted
# without executing anything. Before the fix every line named the cache privilege group instead.
# SYSTEM CLEAR SCHEMA CACHE and SYSTEM CLEAR FORMAT SCHEMA CACHE are absent: their parser cases do
# not accept ON CLUSTER at all.
while read -r command; do
    out=$(run --user "$no_cache_user" --query "$command ON CLUSTER $cluster" 2>&1)
    # The server names each privilege by its enum symbol, so print what it asked for and let the
    # reference hold the expected mapping. The message can appear twice, once from the server log,
    # so stop at the first one.
    echo "${command%% \'*} -> $(sed -n "/necessary to have the grant/{s/.*grant \(.*\) ON \*\.\*.*/\1/p;q;}" <<< "$out")"
done <<EOF
SYSTEM CLEAR DNS CACHE
SYSTEM CLEAR CONNECTIONS CACHE
SYSTEM CLEAR MARK CACHE
SYSTEM CLEAR ICEBERG METADATA CACHE
SYSTEM CLEAR PAIMON METADATA CACHE
SYSTEM CLEAR AVRO SCHEMA CACHE
SYSTEM CLEAR PARQUET METADATA CACHE
SYSTEM CLEAR POINT IN POLYGON CACHE
SYSTEM CLEAR PRIMARY INDEX CACHE
SYSTEM CLEAR MMAP CACHE
SYSTEM CLEAR QUERY CONDITION CACHE
SYSTEM CLEAR ENCRYPTION HEADERS CACHE
SYSTEM CLEAR QUERY CACHE TAG '$tag'
SYSTEM CLEAR COMPILED EXPRESSION CACHE
SYSTEM CLEAR UNCOMPRESSED CACHE
SYSTEM CLEAR INDEX MARK CACHE
SYSTEM CLEAR INDEX UNCOMPRESSED CACHE
SYSTEM CLEAR VECTOR SIMILARITY INDEX CACHE
SYSTEM CLEAR SKIPPING INDEX CACHE
SYSTEM CLEAR TEXT INDEX TOKENS CACHE
SYSTEM CLEAR TEXT INDEX HEADER CACHE
SYSTEM CLEAR TEXT INDEX POSTINGS CACHE
SYSTEM CLEAR TEXT INDEX CACHES
SYSTEM CLEAR FILESYSTEM CACHE '$missing_cache'
SYSTEM SYNC FILESYSTEM CACHE '$missing_cache'
SYSTEM CLEAR PAGE CACHE
SYSTEM CLEAR S3 CLIENT CACHE
SYSTEM CLEAR DISTRIBUTED CACHE
EOF

# SYSTEM CLEAR DISK METADATA CACHE requires the privilege only in private releases, so the two
# builds expect different outcomes and each arm checks its own before reporting the same line.
# The private arm names the privilege instead of accepting any denial, so reverting this case to
# the cache privilege group is caught there too.
disk_metadata_cache_ok() {
    local out expected
    out=$(run --user "$no_cache_user" --query \
        "SYSTEM CLEAR DISK METADATA CACHE $missing_disk ON CLUSTER $cluster" 2>&1)
    if [ "$1" = 0 ]; then
        expected=SUPPORT_IS_DISABLED
    else
        # Split to keep the privilege name out of the check for server-wide commands.
        expected="grant SYSTEM ""DROP FILESYSTEM CACHE ON *.*"
    fi
    if grep -qF "$expected" <<< "$out"; then
        echo "ok"
    else
        echo "FAIL: expected $expected: $out"
    fi
}

disk_metadata_cache_ok "$(${CLICKHOUSE_CLIENT} --query \
    "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")"

${CLICKHOUSE_CLIENT} --query "DROP USER $query_cache_user, $sync_user, $group_user, $no_cache_user"
