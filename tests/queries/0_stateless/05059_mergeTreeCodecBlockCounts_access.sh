#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every column of `mergeTreeCodecBlockCounts` is derived from the source table's data, so reading any of
# them requires `SELECT` on all of its columns. Resolving the structure of the function requires the same.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS t_codec_access;
    DROP TABLE IF EXISTS t_codec_access_log;
    DROP TABLE IF EXISTS t_codec_access_hidden;
    DROP TABLE IF EXISTS t_codec_access_partial;
    DROP TABLE IF EXISTS t_codec_access_dst;

    -- Explicit codecs, CI randomises the server-level default compression codec.
    CREATE TABLE t_codec_access (a UInt64 CODEC(LZ4), b UInt64 CODEC(LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_codec_access SELECT number, number FROM numbers(1000);

    CREATE TABLE t_codec_access_log (a UInt64) ENGINE = Log;

    -- Never granted to the test user, so it stays invisible to it.
    CREATE TABLE t_codec_access_hidden (a UInt64) ENGINE = MergeTree ORDER BY tuple();

    CREATE USER ${username} NOT IDENTIFIED;
"

echo "Without SELECT on the source table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "Without SELECT on the source table, structure only"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With SELECT on a single column of the source table"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a) ON t_codec_access TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With SELECT on every column of the source table"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(b) ON t_codec_access TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT DISTINCT column, mapKeys(codec_block_counts) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access) ORDER BY column;"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access) FORMAT TSV" | cut -f 1

# The engine of the source table is disclosed by `SHOW CREATE TABLE`, which requires `SHOW COLUMNS` on it, so
# it is not something the user below is allowed to learn: at this point it holds neither that privilege nor
# `SELECT` on the table. The access check therefore has to run before the check that rejects a table of another
# engine with `BAD_ARGUMENTS`.

echo "Non-MergeTree source table, without SELECT on it"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_log);" 2>&1 |
    grep -o "ACCESS_DENIED\|BAD_ARGUMENTS" | uniq

echo "Non-MergeTree source table, with SELECT on it"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON t_codec_access_log TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_log);" 2>&1 |
    grep -o "ACCESS_DENIED\|BAD_ARGUMENTS" | uniq

# Which tables exist is not something a user without any privilege on them is allowed to learn, so the check
# on the name has to run before the source table is resolved: an inaccessible table and a missing one answer alike.

echo "Hidden source table, before it is resolved"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_hidden);" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE" | uniq

echo "Missing source table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_missing);" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE" | uniq

echo "Missing source table, for a user who can see the database"
${CLICKHOUSE_CLIENT} --query \
    "DESCRIBE mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_missing);" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE" | uniq

# The same for an ordinary read: the check on the name runs in `StorageMergeTreeCodecBlockCounts::read` as
# well, not only when the structure is resolved, so a plain `SELECT` is not an existence oracle either.

echo "Hidden source table, before it is resolved, on the read path"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_hidden);" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE" | uniq

echo "Missing source table, on the read path"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_missing);" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE" | uniq

echo "Missing source table, on the read path, for a user who can see the database"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_missing);" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE" | uniq

# Analysis-only entrypoints (`EXPLAIN QUERY TREE`, `EXPLAIN SYNTAX`) resolve the table function without reading
# it, so the source table is not resolved on that path and no check runs there. That discloses nothing: the
# structure of this function is a fixed constant and nothing on that path consults the catalog, so the answer is
# the same whether the source table is readable, hidden, or missing. Pinned below, one arm per entrypoint.

# `EXPLAIN QUERY TREE` exists only with the analyzer, and a configuration that turns it off rejects the
# statement with a message that names the source table - which is what the arm below compares. The setting
# is pinned so that the comparison stays about the disclosure and not about which rejection arrived.
explain_as_user() {
    ${CLICKHOUSE_CLIENT} --user="${username}" --enable_analyzer 1 --query \
        "EXPLAIN $1 SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), $2);" 2>&1 | sed "s/$2/SOURCE/g"
}

for kind in "QUERY TREE" "SYNTAX"; do
    readable=$(explain_as_user "${kind}" t_codec_access)
    hidden=$(explain_as_user "${kind}" t_codec_access_hidden)
    missing=$(explain_as_user "${kind}" t_codec_access_missing)
    if [ "${readable}" = "${hidden}" ] && [ "${hidden}" = "${missing}" ]; then
        echo "EXPLAIN ${kind} is the same for a readable, a hidden and a missing source table"
    else
        echo "EXPLAIN ${kind} tells a readable, a hidden and a missing source table apart"
    fi
done

# `EXPLAIN PLAN` and `EXPLAIN PIPELINE` do build the read plan, so they go through the checks.

echo "EXPLAIN PLAN of a hidden source table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "EXPLAIN PLAN SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_hidden);" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE" | uniq

echo "EXPLAIN PIPELINE of a non-MergeTree source table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "EXPLAIN PIPELINE SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_log);" 2>&1 |
    grep -o "ACCESS_DENIED\|BAD_ARGUMENTS" | uniq

# A table function that `canBeUsedToCreateTable` refuses for `CREATE TABLE ... AS f(...)` can still reach a
# persisted definition nested in an argument of another one: both `remote(..., f(...))` and
# `ENGINE = Remote(..., f(...))` keep it in `remote_table_function_ptr`. When the cluster has a local shard,
# both forms resolve the nested function under the creating user's context, through
# `getStructureOfRemoteTableInShard` -> `getActualTableStructureWithAccess`, which is the seam this change adds
# the check to. So the carrier is closed by the check on the source table rather than by the veto, and the arms
# below pin which privilege refuses it, one per tier and one per form. Everything the carrier itself needs is
# granted first, so that what refuses is the source table and not the carrier; `CREATE TABLE` is granted on the
# destination name alone, because a grant on the database would imply `SHOW TABLES` on the hidden table too.

${CLICKHOUSE_CLIENT} -m --query "
    CREATE TABLE t_codec_access_partial (a UInt64 CODEC(LZ4), b UInt64 CODEC(LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_codec_access_partial SELECT number, number FROM numbers(1000);

    GRANT CREATE TABLE, DROP TABLE ON t_codec_access_dst TO ${username};
    GRANT READ, WRITE ON REMOTE TO ${username};
    GRANT TABLE ENGINE ON Remote TO ${username};
    GRANT TABLE ENGINE ON Distributed TO ${username};
    GRANT SELECT(a) ON t_codec_access_partial TO ${username};
"

# Reports the privilege the carrier demanded and the error code, one line each, from a single attempt.
carrier_as_user() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --user="${username}" --query "$1" 2>&1)
    echo "${out}" | grep -o "grant SHOW TABLES\|grant SELECT" | uniq
    echo "${out}" | grep -o "ACCESS_DENIED\|BAD_ARGUMENTS" | uniq
}

echo "Nested in remote(...), with SELECT on a single column of the source table"
carrier_as_user "CREATE TABLE t_codec_access_dst AS remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_partial));"

echo "Nested in a Remote table engine, with SELECT on a single column of the source table"
carrier_as_user "CREATE TABLE t_codec_access_dst ENGINE = Remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_partial));"

# The check on the name runs on this path too, so a carrier is not an existence oracle either.

echo "Nested in remote(...), over a hidden source table"
carrier_as_user "CREATE TABLE t_codec_access_dst AS remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_hidden));"

echo "Nested in a Remote table engine, over a hidden source table"
carrier_as_user "CREATE TABLE t_codec_access_dst ENGINE = Remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_hidden));"

echo "Number of tables the refused carriers left behind"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_codec_access_dst';"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER ${username};
    DROP TABLE t_codec_access;
    DROP TABLE t_codec_access_log;
    DROP TABLE t_codec_access_hidden;
    DROP TABLE t_codec_access_partial;
"
