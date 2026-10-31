#!/usr/bin/env bash
# Tags: no-old-analyzer, no-parallel-replicas
# no-parallel-replicas: reading an ALIAS column with a grant on only that ALIAS column is denied under
# parallel replicas. That is an unrelated pre-existing bug (reproducible on master without this test's
# subject), so exclude the setting rather than encode the wrong behaviour in the reference.

# Column-level SELECT grants must be enforced for columns that the planner resolves away before the
# access check runs, so that PREWHERE cannot be used as an oracle over a column the user cannot read:
#
# 1. An ALIAS column referenced in PREWHERE is replaced by its expression, so it used to never reach
#    the list of selected columns and was not access checked at all, while the same alias in SELECT or
#    WHERE was correctly denied.
# 2. A column used only as an `indexHint` argument is never read, only used for index analysis, but
#    which granules survive the analysis is observable in the result.
#
# In both cases the required privilege is the one on the name written in the query, exactly as in
# WHERE: referencing an ALIAS column requires a grant on the ALIAS column itself, and a grant on the
# physical columns its expression reads is neither sufficient nor required. Those source columns are
# still read from disk to compute the ALIAS - the administrator authored the ALIAS expression and so
# chose what it exposes - but their values never reach the user.

# Most of the queries below are expected to be denied, and each denial would otherwise add an <Error>
# log line and a stack trace to the output, because the harness streams server-side logs to the client.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_PUB="user_pub_${CLICKHOUSE_DATABASE}"
USER_ALIAS="user_alias_${CLICKHOUSE_DATABASE}"
USER_SOURCE="user_source_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${USER_PUB}, ${USER_ALIAS}, ${USER_SOURCE};

    CREATE TABLE t_prewhere_alias
    (
        secret Int32,
        pub Int32,
        secret_alias Int32 ALIAS secret,
        secret_alias_expression Int32 ALIAS secret_alias + 1
    )
    ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

    INSERT INTO t_prewhere_alias (secret, pub) SELECT number * 100, number FROM numbers(8);

    CREATE USER ${USER_PUB}, ${USER_ALIAS}, ${USER_SOURCE};
    GRANT SELECT(pub) ON ${CLICKHOUSE_DATABASE}.t_prewhere_alias TO ${USER_PUB};
    GRANT SELECT(pub, secret_alias, secret_alias_expression) ON ${CLICKHOUSE_DATABASE}.t_prewhere_alias TO ${USER_ALIAS};
    GRANT SELECT(pub, secret) ON ${CLICKHOUSE_DATABASE}.t_prewhere_alias TO ${USER_SOURCE};
"

# Run all the queries of one user in a single session, echoing each query before its own result so
# that every line of the reference is attributable. --ignore-error keeps the session going past the
# ACCESS_DENIED answers that most of these queries are expected to produce; without it the first
# denial would end the session and silently drop every query after it.
#
# An exception is printed by the client as exactly three lines - the "Received exception" banner, the
# "Code: ..." message and the echoed query - of which only the error name is stable: the banner holds
# the server version and the message holds the user name, which embeds ${CLICKHOUSE_DATABASE}. Reduce
# the block to that name, keeping it distinct per error so that a change of error shows up as a diff
# rather than being masked.
run_all()
{
    local user=$1
    shift

    local sql=""
    local query
    for query in "$@"
    do
        # The echoed query is derived from the query itself, so the two cannot drift apart.
        sql+="SELECT '${query//\'/\'\'}';
${query};
"
    done

    ${CLICKHOUSE_CLIENT} --user "${user}" --multiquery --ignore-error --query "${sql}" 2>&1 \
        | sed -E '/^Received exception/d; /^\(query:/d; s/^Code: [0-9]+\..*\(([A-Z_]+)\)$/\1/'
}

echo "-- user granted only pub"
run_all "${USER_PUB}" \
    "SELECT arraySort(groupArray(pub)) FROM t_prewhere_alias" \
    "SELECT pub FROM t_prewhere_alias PREWHERE pub = 2" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret = 200" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret_alias = 200" \
    "SELECT count() FROM t_prewhere_alias PREWHERE secret_alias = 200" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret_alias_expression = 201" \
    "SELECT arraySort(groupArray(pub)) FROM t_prewhere_alias WHERE indexHint(secret >= 500)" \
    "SELECT arraySort(groupArray(pub)) FROM t_prewhere_alias WHERE indexHint(secret_alias >= 500)" \
    "SELECT count() FROM t_prewhere_alias WHERE pub = 5 AND indexHint(secret < 500)" \
    "SELECT count() FROM t_prewhere_alias WHERE pub = 5 AND indexHint(pub < 5)"

echo "-- user granted pub and the alias columns, but not their source"
run_all "${USER_ALIAS}" \
    "SELECT secret_alias FROM t_prewhere_alias PREWHERE pub = 2" \
    "SELECT pub FROM t_prewhere_alias WHERE secret_alias = 200" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret_alias = 200" \
    "SELECT pub FROM t_prewhere_alias WHERE secret_alias_expression = 201" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret_alias_expression = 201" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret_alias_expression + 0 = 201" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret = 200" \
    "SELECT arraySort(groupArray(pub)) FROM t_prewhere_alias WHERE indexHint(secret >= 500)"

echo "-- user granted pub and the source column, but not the alias columns"
run_all "${USER_SOURCE}" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret = 200" \
    "SELECT pub FROM t_prewhere_alias WHERE secret_alias = 200" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret_alias = 200" \
    "SELECT pub FROM t_prewhere_alias WHERE secret_alias_expression = 201" \
    "SELECT pub FROM t_prewhere_alias PREWHERE secret_alias_expression = 201" \
    "SELECT arraySort(groupArray(pub)) FROM t_prewhere_alias WHERE indexHint(secret >= 500)" \
    "SELECT count() FROM t_prewhere_alias WHERE pub = 5 AND indexHint(secret < 500)" \
    "SELECT count() FROM t_prewhere_alias WHERE pub = 5 AND indexHint(secret < 501)"

# A row policy is defined by an administrator, so it may reference columns the user cannot read.
echo "-- row policy over a column the user is not granted"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY p_prewhere_alias ON ${CLICKHOUSE_DATABASE}.t_prewhere_alias USING secret_alias_expression != 201 TO ${USER_PUB}"
run_all "${USER_PUB}" \
    "SELECT arraySort(groupArray(pub)) FROM t_prewhere_alias" \
    "SELECT arraySort(groupArray(pub)) FROM t_prewhere_alias PREWHERE pub > 1"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP ROW POLICY p_prewhere_alias ON ${CLICKHOUSE_DATABASE}.t_prewhere_alias;
    DROP TABLE t_prewhere_alias;
    DROP USER ${USER_PUB}, ${USER_ALIAS}, ${USER_SOURCE};
"
