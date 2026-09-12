#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libmysqlclient (MySQL database engine), which is not built in fast test.
#   no-parallel: attaches a MySQL database pointing at an unreachable endpoint. Because
#     `show_remote_databases_in_system_tables` defaults to `true`, the database is visible in
#     `system.tables` and `system.columns`, so any concurrent query that scans those tables
#     without a database filter would try to connect to the unreachable endpoint and fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# The connection errors that the listing probes produce are logged server-side at error level;
# keep them out of the test's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A source database hidden from the caller that fails while being listed must not change what the
# facade lists compared to a hidden source that is healthy: otherwise the listing is an oracle for
# the state of a source the caller may not see. A hidden source that owns a name also shadows the
# same name in a later source — the read path stops at the first source that owns the name — so the
# walk has to stop at the failing source instead of falling through to the later ones.
#
# The discriminator is therefore not "the query failed" (it fails in every arm), but the listing at
# a *fixed* grant set compared across three facades whose first source is hidden from the caller:
#   * broken   — an unreachable `MySQL` source that also defines `t`
#   * healthy  — a healthy source that defines `t` (so `t` is shadowed and stays invisible)
#   * no-`t`   — a healthy source that does not define `t` (negative control: `t` must stay listed)
# The first two must produce the same listing; the third must still show and read the later source.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_BROKEN="db_broken_${SUF}"     # hidden, unreachable, defines `t`
DB_HIDDEN_T="db_hidden_t_${SUF}" # hidden, healthy, defines `t`
DB_HIDDEN_NO_T="db_hidden_no_t_${SUF}" # hidden, healthy, does not define `t`
DB_VISIBLE="db_visible_${SUF}"   # granted to the caller, defines `t`
OVL_BROKEN="ovl_broken_${SUF}"
OVL_HEALTHY="ovl_healthy_${SUF}"
OVL_NO_T="ovl_no_t_${SUF}"
USER_OVL="u_ovl_${SUF}"          # granted on the facades and on the later source only
USER_DUAL="u_dual_${SUF}"        # additionally granted on the hidden sources

# `CREATE DATABASE ... ENGINE = MySQL` validates the connection eagerly, so ATTACH is used to
# register a source whose endpoint is unreachable, modelling a source that went down after it was
# attached. Port 1 on localhost is never listening, so every probe fails instantly.
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${OVL_BROKEN};
    DROP DATABASE IF EXISTS ${OVL_HEALTHY};
    DROP DATABASE IF EXISTS ${OVL_NO_T};
    DROP DATABASE IF EXISTS ${DB_BROKEN};
    DROP DATABASE IF EXISTS ${DB_HIDDEN_T};
    DROP DATABASE IF EXISTS ${DB_HIDDEN_NO_T};
    DROP DATABASE IF EXISTS ${DB_VISIBLE};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    ATTACH DATABASE ${DB_BROKEN} ENGINE = MySQL('127.0.0.1:1', 'fake_db', 'user', 'password');

    CREATE DATABASE ${DB_HIDDEN_T};
    CREATE TABLE ${DB_HIDDEN_T}.t (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${DB_HIDDEN_T}.t VALUES (11);

    CREATE DATABASE ${DB_HIDDEN_NO_T};
    CREATE TABLE ${DB_HIDDEN_NO_T}.other (x UInt8) ENGINE = MergeTree ORDER BY x;

    CREATE DATABASE ${DB_VISIBLE};
    CREATE TABLE ${DB_VISIBLE}.t (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${DB_VISIBLE}.t VALUES (22);

    CREATE DATABASE ${OVL_BROKEN} ENGINE = Overlay('${DB_BROKEN}', '${DB_VISIBLE}');
    CREATE DATABASE ${OVL_HEALTHY} ENGINE = Overlay('${DB_HIDDEN_T}', '${DB_VISIBLE}');
    CREATE DATABASE ${OVL_NO_T} ENGINE = Overlay('${DB_HIDDEN_NO_T}', '${DB_VISIBLE}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW, SELECT ON ${OVL_BROKEN}.* TO ${USER_OVL}, ${USER_DUAL};
    GRANT SHOW, SELECT ON ${OVL_HEALTHY}.* TO ${USER_OVL}, ${USER_DUAL};
    GRANT SHOW, SELECT ON ${OVL_NO_T}.* TO ${USER_OVL}, ${USER_DUAL};
    GRANT SHOW, SELECT ON ${DB_VISIBLE}.* TO ${USER_OVL}, ${USER_DUAL};

    GRANT SHOW, SELECT ON ${DB_BROKEN}.* TO ${USER_DUAL};
    GRANT SHOW, SELECT ON ${DB_HIDDEN_T}.* TO ${USER_DUAL};
    GRANT SHOW, SELECT ON ${DB_HIDDEN_NO_T}.* TO ${USER_DUAL};
"

echo 'Hidden broken source: SHOW TABLES lists nothing, the same as for a hidden healthy source'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW TABLES FROM ${OVL_BROKEN}"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW TABLES FROM ${OVL_HEALTHY}"

echo 'Hidden broken source: system.tables and system.columns agree with the healthy-source facade'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() FROM system.tables WHERE database = '${OVL_BROKEN}'"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() FROM system.tables WHERE database = '${OVL_HEALTHY}'"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() FROM system.columns WHERE database = '${OVL_BROKEN}'"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() FROM system.columns WHERE database = '${OVL_HEALTHY}'"

echo 'Hidden broken source: the read path stays denied, matching the listing'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS TABLE ${OVL_BROKEN}.t"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT x FROM ${OVL_BROKEN}.t" 2>&1 | grep -o -m1 ACCESS_DENIED

echo 'Negative control: a hidden source without the name keeps the later source listed and readable'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW TABLES FROM ${OVL_NO_T}"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS TABLE ${OVL_NO_T}.t"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT x FROM ${OVL_NO_T}.t"

echo 'Precedence is intact for a caller granted on both sources'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SHOW TABLES FROM ${OVL_HEALTHY}"
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT x FROM ${OVL_HEALTHY}.t"

echo 'A caller granted on the broken source still sees its own error'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SHOW TABLES FROM ${OVL_BROKEN}" 2>&1 | grep -o -m1 ALL_CONNECTION_TRIES_FAILED

echo 'Listing the later source directly is unaffected'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW TABLES FROM ${DB_VISIBLE}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${OVL_BROKEN};
    DROP DATABASE ${OVL_HEALTHY};
    DROP DATABASE ${OVL_NO_T};
    DROP DATABASE ${DB_BROKEN};
    DROP DATABASE ${DB_HIDDEN_T};
    DROP DATABASE ${DB_HIDDEN_NO_T};
    DROP DATABASE ${DB_VISIBLE};
    DROP USER ${USER_OVL};
    DROP USER ${USER_DUAL};
"
