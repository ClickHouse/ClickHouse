#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: `DETACH DATABASE` of the extra database and `lazy_load_tables` need a
# plain `Atomic` database.

# `canHideRows` must classify the storage that actually serves the read, not the object that masks
# it. A `TableProxy` (a lazily loaded table of a database with `lazy_load_tables = 1`) reports
# neither `isView` nor its real engine until the table is materialized, and an `Alias` table
# forwards `read` to its target while reporting the engine `Alias`. If either mask made a `Merge`
# wrapper look row-preserving, a `SQL SECURITY DEFINER` view over it would lose its barrier and
# plan exactly like its `SQL SECURITY INVOKER` twin.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
lazy_db="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} <<EOF
DROP DATABASE IF EXISTS $lazy_db;
CREATE DATABASE $lazy_db ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE $lazy_db.secret (key UInt64, owner String, val String)
ENGINE = MergeTree ORDER BY key;
INSERT INTO $lazy_db.secret SELECT number, 'nobody', toString(number) FROM numbers(1000);

-- The row hiding lives below the \`Merge\` wrapper, and the wrapper is what the proxy masks.
CREATE VIEW $lazy_db.inner_v
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $lazy_db.secret WHERE owner = currentUser();

CREATE TABLE $lazy_db.wrapper (key UInt64, owner String, val String)
ENGINE = Merge('$lazy_db', '^inner_v\$');

CREATE VIEW $lazy_db.v_definer
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $lazy_db.wrapper;

CREATE VIEW $lazy_db.v_invoker
SQL SECURITY INVOKER
AS SELECT * FROM $lazy_db.wrapper;

DETACH DATABASE $lazy_db;
ATTACH DATABASE $lazy_db;
EOF

explain_client="${CLICKHOUSE_CLIENT} --enable_parallel_replicas 0
    --query_plan_merge_filters 1 --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0"

echo "===== the lazy proxy masks the Merge wrapper ====="
${CLICKHOUSE_CLIENT} --query \
    "SELECT engine FROM system.tables WHERE database = '$lazy_db' AND name = 'wrapper'"

echo "===== a definer view over a lazily proxied wrapper stays a barrier ====="
# Planning either view materializes the proxy, after which nothing is masked anymore — so the mask
# is re-established before each round, and the `DEFINER` view is planned strictly first, while the
# wrapper still reports `TableProxy`.
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    ${CLICKHOUSE_CLIENT} --query "DETACH DATABASE $lazy_db"
    ${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE $lazy_db"
    # shellcheck disable=SC2086
    ${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $lazy_db.v_definer WHERE val = 'x'" > "${CLICKHOUSE_TMP}/04827_definer.txt" 2>&1
    # shellcheck disable=SC2086
    ${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $lazy_db.v_invoker WHERE val = 'x'" > "${CLICKHOUSE_TMP}/04827_invoker.txt" 2>&1
    if diff -q "${CLICKHOUSE_TMP}/04827_definer.txt" "${CLICKHOUSE_TMP}/04827_invoker.txt" > /dev/null
    then echo "same"; else echo "different"; fi
done

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secret (key UInt64, owner String, val String)
ENGINE = MergeTree ORDER BY key;
INSERT INTO $db.secret SELECT number, 'nobody', toString(number) FROM numbers(1000);

CREATE VIEW $db.inner_v
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.secret WHERE owner = currentUser();

CREATE TABLE $db.wrapper (key UInt64, owner String, val String)
ENGINE = Merge('$db', '^inner_v\$');

CREATE TABLE $db.alias_w ENGINE = Alias('$db', 'wrapper');

CREATE VIEW $db.v_definer
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.alias_w;

CREATE VIEW $db.v_invoker
SQL SECURITY INVOKER
AS SELECT * FROM $db.alias_w;
EOF

echo "===== a definer view over an alias of a wrapper stays a barrier ====="
for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    if diff -q \
        <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.v_definer WHERE val = 'x'" 2>&1) \
        <(${explain_client} ${analyzer_settings} --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.v_invoker WHERE val = 'x'" 2>&1) > /dev/null
    then echo "same"; else echo "different"; fi
done

${CLICKHOUSE_CLIENT} --query "DROP DATABASE $lazy_db"
