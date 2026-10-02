#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In `clickhouse-local`, an explicitly configured database (the `--database` option or a
# config-file `database` key) must win over a `database` setting inherited from a profile;
# without an explicit choice, the profile value keeps working as the default choice.

DIR="${CLICKHOUSE_TMP}/04816"
mkdir -p "${DIR}"

cat > "${DIR}/users.xml" <<INNER_EOF
<clickhouse>
    <profiles>
        <default>
            <database>system</database>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas>
        <default></default>
    </quotas>
</clickhouse>
INNER_EOF

cat > "${DIR}/profile_only.xml" <<INNER_EOF
<clickhouse>
    <users_config>users.xml</users_config>
</clickhouse>
INNER_EOF

cat > "${DIR}/profile_and_database.xml" <<INNER_EOF
<clickhouse>
    <users_config>users.xml</users_config>
    <database>default</database>
</clickhouse>
INNER_EOF

echo "-- profile database only: the profile value is the default choice"
${CLICKHOUSE_LOCAL} --config-file "${DIR}/profile_only.xml" --query "SELECT currentDatabase()"

echo "-- config-file database key: wins over the profile database setting"
${CLICKHOUSE_LOCAL} --config-file "${DIR}/profile_and_database.xml" --query "SELECT currentDatabase()"

echo "-- --database option: wins over the profile database setting"
${CLICKHOUSE_LOCAL} --config-file "${DIR}/profile_only.xml" --database default --query "SELECT currentDatabase()"

rm -r "${DIR}"
