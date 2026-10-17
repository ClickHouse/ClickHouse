#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_url"
remote_nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_remote"
header_nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_headers"
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"
role="${CLICKHOUSE_TEST_UNIQUE_NAME}_role"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP DICTIONARY IF EXISTS override_access_dict;
        DROP TABLE IF EXISTS override_access_source;
        DROP USER IF EXISTS $user;
        DROP ROLE IF EXISTS $role;
        DROP NAMED COLLECTION IF EXISTS $nc;
        DROP NAMED COLLECTION IF EXISTS $remote_nc;
        DROP NAMED COLLECTION IF EXISTS $header_nc;
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE NAMED COLLECTION $nc AS
        url = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1',
        format = 'TSV' OVERRIDABLE;
    CREATE NAMED COLLECTION $remote_nc AS
        host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, database = '${CLICKHOUSE_DATABASE}',
        user = 'default', password = '';
    CREATE NAMED COLLECTION $header_nc AS
        url = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', format = 'TSV',
        \`headers.header.name\` = 'Accept', \`headers.header.value\` = 'text/plain';
    CREATE USER $user;
    CREATE ROLE $role;
    GRANT $role TO $user;
    GRANT URL, REMOTE, CREATE TEMPORARY TABLE ON *.* TO $user;
    GRANT NAMED COLLECTION ON $nc TO $user;
    GRANT NAMED COLLECTION ON $remote_nc TO $user;
    GRANT NAMED COLLECTION ON $header_nc TO $user;
"

echo 'Usage and additional keys need no secrets privilege'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    SELECT * FROM url($nc);
    SELECT * FROM url($nc, structure = 'value UInt64');
    SELECT * FROM url($nc, structure = 'value UInt8', structure = 'value UInt64');
    SELECT * FROM url($nc, structure = 'value UInt64', headers('Accept' = 'text/plain'));
    SELECT * FROM url($nc, url = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+2', structure = 'value UInt64'); -- { serverError ACCESS_DENIED }
    SELECT * FROM url($nc, format = 'CSV', structure = 'value UInt64'); -- { serverError ACCESS_DENIED }
    SELECT * FROM url($header_nc, structure = 'value UInt64', headers('Accept' = 'text/csv')); -- { serverError ACCESS_DENIED }
    SELECT * FROM url($header_nc, structure = 'value UInt64', \`headers.header[0].value\` = 'text/csv'); -- { serverError ACCESS_DENIED }

    SELECT * FROM remote($remote_nc, host = '127.0.0.1', table = 'one'); -- { serverError ACCESS_DENIED }
    SELECT * FROM remote($remote_nc, hostname = '127.0.0.1', table = 'one'); -- { serverError ACCESS_DENIED }
    SELECT * FROM remote($remote_nc, addresses_expr = '127.0.0.1:1', table = 'one'); -- { serverError ACCESS_DENIED }
    SELECT * FROM remote($remote_nc, db = 'system', table = 'one'); -- { serverError ACCESS_DENIED }
    SELECT * FROM remote($remote_nc, username = 'other', table = 'one'); -- { serverError ACCESS_DENIED }
    SELECT * FROM remote($remote_nc, database = numbers(1)); -- { serverError ACCESS_DENIED }
"

echo 'The secrets privilege is scoped to the collection and inherited from roles'
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON $nc TO $role"
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    SET format_display_secrets_in_show_and_select = 0;
    SELECT * FROM url($nc, url = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+2', structure = 'value UInt64');
    SELECT * FROM url($nc, format = 'CSV', structure = 'value UInt64');
    SELECT * FROM remote($remote_nc, addresses_expr = '127.0.0.1:1', table = 'one'); -- { serverError ACCESS_DENIED }
    SELECT * FROM url($header_nc, structure = 'value UInt64', headers('Accept' = 'text/csv')); -- { serverError ACCESS_DENIED }
"

${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON $header_nc TO $role"
${CLICKHOUSE_CLIENT} --user "$user" --query "
    SELECT * FROM url($header_nc, structure = 'value UInt64', headers('Accept' = 'text/csv'));
"

${CLICKHOUSE_CLIENT} --query "ALTER NAMED COLLECTION $nc SET format = 'TSV' NOT OVERRIDABLE"
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    SELECT * FROM url($nc, format = 'CSV', structure = 'value UInt64'); -- { serverError BAD_ARGUMENTS }
"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW NAMED COLLECTIONS SECRETS ON $nc FROM $role"
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    SELECT * FROM url($nc, url = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+2', structure = 'value UInt64'); -- { serverError ACCESS_DENIED }
"

echo 'Dictionary sources can add keys'
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE override_access_source (id UInt64, value UInt64) ENGINE = Memory;
    INSERT INTO override_access_source VALUES (1, 42);
    CREATE DICTIONARY override_access_dict (id UInt64, value UInt64)
    PRIMARY KEY id SOURCE(CLICKHOUSE(NAME $remote_nc TABLE 'override_access_source'))
    LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('override_access_dict', 'value', toUInt64(1));
"
