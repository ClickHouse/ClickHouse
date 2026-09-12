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

# `CREATE TABLE ... AS mergeTreeCodecBlockCounts(...)` stores the function and materialises it under the global
# context, lazily, on the first read of the created table. The checks therefore have to run when the created table
# is read, under the reader's context: otherwise a user who may read the created table but not the source could
# tell a hidden source from a missing one, and learn its engine once it is recreated under the same name.

${CLICKHOUSE_CLIENT} -m --query "
    CREATE TABLE t_codec_access_dst AS mergeTreeCodecBlockCounts(currentDatabase(), t_codec_access_hidden);
    GRANT SELECT ON t_codec_access_dst TO ${username};
"

echo "Through a table created from the function, hidden source table"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT count() FROM t_codec_access_dst;" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE\|BAD_ARGUMENTS" | uniq

echo "Through a table created from the function, missing source table"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_codec_access_hidden;"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT count() FROM t_codec_access_dst;" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE\|BAD_ARGUMENTS" | uniq

echo "Through a table created from the function, non-MergeTree source table recreated, without SELECT on it"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_codec_access_hidden (a UInt64) ENGINE = Log;"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT count() FROM t_codec_access_dst;" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE\|BAD_ARGUMENTS" | uniq

echo "Through a table created from the function, non-MergeTree source table recreated, with SELECT on it"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON t_codec_access_hidden TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT count() FROM t_codec_access_dst;" 2>&1 |
    grep -o "ACCESS_DENIED\|UNKNOWN_TABLE\|BAD_ARGUMENTS" | uniq

echo "Through a table created from the function, MergeTree source table recreated, with SELECT on it"
${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE t_codec_access_hidden;
    CREATE TABLE t_codec_access_hidden (a UInt64 CODEC(LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO t_codec_access_hidden SELECT number FROM numbers(1000);
"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT DISTINCT column, mapKeys(codec_block_counts) FROM t_codec_access_dst ORDER BY column;"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER ${username};
    DROP TABLE t_codec_access;
    DROP TABLE t_codec_access_log;
    DROP TABLE t_codec_access_hidden;
    DROP TABLE t_codec_access_dst;
"
