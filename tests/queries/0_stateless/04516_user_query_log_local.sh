#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_dir="${CLICKHOUSE_TMP}/04516_user_query_log_local_${CLICKHOUSE_DATABASE}"
config="${test_dir}/config.xml"

rm -rf "${test_dir}"
mkdir -p "${test_dir}/data" "${test_dir}/tmp" "${test_dir}/user_files" "${test_dir}/format_schemas"

make_config()
{
    cat > "${config}" <<EOF
<clickhouse>
    <path>${test_dir}/data/</path>
    <tmp_path>${test_dir}/tmp/</tmp_path>
    <user_files_path>${test_dir}/user_files/</user_files_path>
    <format_schema_path>${test_dir}/format_schemas/</format_schema_path>
    <logger>
        <level>none</level>
        <console>false</console>
    </logger>
    <query_log>
        <database>system</database>
        <table>$1</table>
        <engine>ENGINE = MergeTree PARTITION BY event_date ORDER BY event_time</engine>
        <enable_user_query_log>$2</enable_user_query_log>
    </query_log>
</clickhouse>
EOF
}

make_config_without_query_log()
{
    cat > "${config}" <<EOF
<clickhouse>
    <path>${test_dir}/data/</path>
    <tmp_path>${test_dir}/tmp/</tmp_path>
    <user_files_path>${test_dir}/user_files/</user_files_path>
    <format_schema_path>${test_dir}/format_schemas/</format_schema_path>
    <logger>
        <level>none</level>
        <console>false</console>
    </logger>
</clickhouse>
EOF
}

# Without a configured query log the table exists and is empty, not an error.
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM system.user_query_log"
${CLICKHOUSE_LOCAL} --query "SELECT engine FROM system.tables WHERE database = 'system' AND name = 'user_query_log'"

# With a configured query log the current user sees their own records.
make_config custom_query_log true
${CLICKHOUSE_LOCAL} --config-file "${config}" --log_queries 1 --query "
    SELECT 1 FORMAT Null;
    SYSTEM FLUSH LOGS query_log;
    SELECT count() >= 1, countIf(if(initial_user != '', initial_user, user) != currentUser()) FROM system.user_query_log;
"

# The implicit SELECT grant on `system.user_query_log` is computed in clickhouse-local as well
# (it initializes the access control on its own path, separately from the server):
# a user without any explicit grants can read the table.
${CLICKHOUSE_LOCAL} --query "
    CREATE USER user_04516;
    SHOW GRANTS FOR user_04516 WITH IMPLICIT;
" | grep -c "GRANT SELECT ON system.user_query_log TO user_04516"

# The table can be disabled.
make_config custom_query_log false
if ${CLICKHOUSE_LOCAL} --config-file "${config}" --query "SELECT count() FROM system.user_query_log" >/dev/null 2>&1
then
    echo "UNEXPECTED"
else
    echo "disabled"
fi

# And when it is disabled, there is no implicit grant on the name: it may back a regular table.
${CLICKHOUSE_LOCAL} --config-file "${config}" --query "
    CREATE USER user_04516;
    SHOW GRANTS FOR user_04516 WITH IMPLICIT;
" | grep -c "GRANT SELECT ON system.user_query_log" || true

# The query log itself cannot be configured to flush into `system.user_query_log`.
make_config user_query_log true
if ${CLICKHOUSE_LOCAL} --config-file "${config}" --query "SELECT 1" 2>&1 | grep -q "cannot be set to"
then
    echo "config rejected"
else
    echo "UNEXPECTED"
fi

# In `--only-system-tables` mode the system loggers are not started, so the live query log object is
# absent, but the persisted query log table is still loaded from disk. `system.user_query_log` must
# resolve the configured backing table and read it, instead of treating "logger not running" as "no
# query log configured" and silently returning an empty result in the mode meant for reading existing
# system tables.
rm -rf "${test_dir}"
mkdir -p "${test_dir}/data/metadata/system" "${test_dir}/tmp" "${test_dir}/user_files" "${test_dir}/format_schemas"
# A persistent `system` database, as a running server leaves behind, so the query log table survives
# across invocations (the `system` database that clickhouse-local creates on its own is ephemeral).
echo "ATTACH DATABASE system ENGINE=Ordinary" > "${test_dir}/data/metadata/system.sql"
make_config query_log true
# First invocation: produce and persist query log records (the loggers run here).
${CLICKHOUSE_LOCAL} --config-file "${config}" --log_queries 1 --query "
    SELECT 1 FORMAT Null;
    SYSTEM FLUSH LOGS query_log;
    SELECT count() >= 1 FROM system.user_query_log;
"
# Second invocation: only load the persisted system tables (the loggers are skipped). The current user
# still sees their own persisted records instead of an empty result.
${CLICKHOUSE_LOCAL} --config-file "${config}" --only-system-tables --query "
    SELECT count() >= 1, countIf(if(initial_user != '', initial_user, user) != currentUser()) FROM system.user_query_log;
"
# But the resolution of the backing table applies only to a configured query log: with the query log
# section removed, the persisted table is still attached from disk, while `system.user_query_log` must
# be empty, because the query log is not configured any more.
make_config_without_query_log
${CLICKHOUSE_LOCAL} --config-file "${config}" --only-system-tables --query "
    SELECT count() FROM system.user_query_log;
"

# `EXPLAIN ANALYZE` executes the pipeline and accounts every executed processor to a step of the plan
# it explains, so the processors of the internal query must be accounted to the step that stands for
# the read, instead of the steps of the internal query plan, which the explained plan knows nothing
# about.
rm -rf "${test_dir}"
mkdir -p "${test_dir}/data" "${test_dir}/tmp" "${test_dir}/user_files" "${test_dir}/format_schemas"
make_config custom_query_log true
if ${CLICKHOUSE_LOCAL} --config-file "${config}" --log_queries 1 --query "
    SELECT 1 FORMAT Null;
    SYSTEM FLUSH LOGS query_log;
    EXPLAIN ANALYZE SELECT count() FROM system.user_query_log;
" 2>&1 | grep -q "ReadFromUserQueryLog"
then
    echo "explain analyze"
else
    echo "UNEXPECTED"
fi

# A leftover table with this name (e.g. created before an upgrade) is reported on startup.
rm -rf "${test_dir}"
mkdir -p "${test_dir}/metadata/system"
echo "ATTACH DATABASE system ENGINE=Ordinary" > "${test_dir}/metadata/system.sql"
echo "ATTACH TABLE system.user_query_log (x UInt8) ENGINE = Memory;" > "${test_dir}/metadata/system/user_query_log.sql"
if ${CLICKHOUSE_LOCAL} --path "${test_dir}" --query "SELECT 1" 2>&1 | grep -q "already exists"
then
    echo "existing table reported"
else
    echo "UNEXPECTED"
fi

rm -rf "${test_dir}"
