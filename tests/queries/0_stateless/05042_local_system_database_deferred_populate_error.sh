#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A failed deferred population of the `system` database in `clickhouse local` poisons the database for good:
# every later access reports the same error, including lookups of the tables the populator managed to attach
# before it threw. Serving those would expose an observably half-populated database.

test_dir="${CLICKHOUSE_TMP}/05042_${CLICKHOUSE_DATABASE}"

rm -rf "${test_dir}"
mkdir -p "${test_dir}/metadata/system" "${test_dir}/data/system/settings"

# A stored table occupies the name of a deferred system table, so the population throws `TABLE_ALREADY_EXISTS`
# partway through, after `system.numbers` has already been attached.
echo "ATTACH DATABASE system ENGINE=Ordinary" > "${test_dir}/metadata/system.sql"
echo "ATTACH TABLE system.settings (x UInt8) ENGINE = MergeTree ORDER BY x;" > "${test_dir}/metadata/system/settings.sql"

# The population fails with the collision.
${CLICKHOUSE_LOCAL} --path "${test_dir}" --query "SELECT * FROM system.settings" 2>&1 | grep -c "already exists"

# Everything after the failed population must rethrow the remembered error instead of serving what got attached
# before the failure: `system.numbers` from the failed populator, `system.one` attached eagerly at startup, the
# implicit `system.one` of a `FROM`-less query, and `EXISTS`, which goes through `isTableExist`. `--ignore-error`
# keeps the session going after each error but swallows the error text, so the queries that must fail are the
# queries that must print nothing.
${CLICKHOUSE_LOCAL} --path "${test_dir}" --multiquery --ignore-error --query "
    SELECT 'before the failure';
    SELECT * FROM system.settings;
    SELECT 'half-attached table served', count() FROM (SELECT * FROM system.numbers LIMIT 1);
    SELECT 'eager table served' FROM system.one;
    SELECT 'implicit system.one served';
    EXISTS TABLE system.numbers;
    SHOW TABLES FROM system;
"

rm -rf "${test_dir}"
