#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A SQL-defined user whose constraint leaves out a bound of the default profile still gets that bound,
# because the default profile comes first in its chain, so it must be able to log in. When the bound comes
# from a separate system profile, which is not in the chain, leaving it out lifts it, so the login is refused.
# The views run as their definer, which logs in as that user.

DIR="${CLICKHOUSE_TMP}/05385"
mkdir -p "${DIR}"
trap 'rm -r "${DIR}"' EXIT

write_users_xml()
{
    cat > "${DIR}/users_$1.xml" <<INNER_EOF
<clickhouse>
    <profiles>
        <default>$2</default>
        <system>$3</system>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>
    <quotas>
        <default></default>
    </quotas>
</clickhouse>
INNER_EOF

    cat > "${DIR}/config_$1.xml" <<INNER_EOF
<clickhouse>
    <users_config>users_$1.xml</users_config>
    <system_profile>$4</system_profile>
</clickhouse>
INNER_EOF
}

BOUNDS="<constraints><max_execution_time><min>1</min><max>100</max></max_execution_time></constraints>"
write_users_xml in_chain "${BOUNDS}" "" default
write_users_xml system_profile "" "${BOUNDS}" system

QUERY="
    CREATE SETTINGS PROFILE p_max SETTINGS max_execution_time MAX 60;
    CREATE USER u_max IDENTIFIED WITH no_password SETTINGS max_execution_time MAX 50;
    CREATE USER u_both IDENTIFIED WITH no_password SETTINGS max_execution_time MIN 2 MAX 50;
    CREATE USER u_cir IDENTIFIED WITH no_password SETTINGS max_execution_time CHANGEABLE_IN_READONLY;
    CREATE USER u_profile IDENTIFIED WITH no_password SETTINGS PROFILE p_max;
    GRANT SELECT ON system.settings TO u_max, u_both, u_cir, u_profile;
    CREATE VIEW v_max DEFINER = u_max SQL SECURITY DEFINER AS SELECT min, max FROM system.settings WHERE name = 'max_execution_time';
    CREATE VIEW v_both DEFINER = u_both SQL SECURITY DEFINER AS SELECT min, max FROM system.settings WHERE name = 'max_execution_time';
    CREATE VIEW v_cir DEFINER = u_cir SQL SECURITY DEFINER AS SELECT min, max FROM system.settings WHERE name = 'max_execution_time';
    CREATE VIEW v_profile DEFINER = u_profile SQL SECURITY DEFINER AS SELECT min, max FROM system.settings WHERE name = 'max_execution_time';
    SELECT 'MAX 50'; SELECT * FROM v_max;
    SELECT 'MIN 2 MAX 50'; SELECT * FROM v_both;
    SELECT 'CHANGEABLE_IN_READONLY'; SELECT * FROM v_cir;
    SELECT 'PROFILE MAX 60'; SELECT * FROM v_profile;
"

# Print the labels, the bounds the definer gets and the reason a login is refused.
run_query()
{
    ${CLICKHOUSE_LOCAL} --config-file "$1" --ignore-error -n --query "${QUERY}" 2>&1 \
        | sed -n -E 's/^Code: [0-9]+\. DB::Exception: //p; /^[A-Z0-9][A-Z0-9_ ]*$/p; /^[0-9]+\t[0-9]+$/p'
}

echo "-- the bound comes from the default profile"
run_query "${DIR}/config_in_chain.xml"

echo "-- the bound comes from a separate system profile"
run_query "${DIR}/config_system_profile.xml"
