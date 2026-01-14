#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Overrides
TEST_DATABASE=$CLICKHOUSE_DATABASE
TEST_HOST=${CLICKHOUSE_HOST:-"localhost"}
TEST_PORT=${CLICKHOUSE_PORT_TCP:-9000}
CLICKHOUSE_DATABASE="system"
CLICKHOUSE_HOST=""
CLICKHOUSE_PORT_TCP=""

# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG=$CLICKHOUSE_TMP/client.xml
cat > $CONFIG <<EOL
<clickhouse>
    <host>$TEST_HOST</host>
    <port>$TEST_PORT</port>
    <database>$TEST_DATABASE</database>

    <connections_credentials>
        <connection>
            <name>test_hostname_invalid</name>
            <hostname>MySQL</hostname>
        </connection>

        <connection>
            <name>$TEST_HOST</name>
            <database>system</database>
        </connection>

        <connection>
            <name>test_port</name>
            <hostname>$TEST_HOST</hostname>
            <port>0</port>
        </connection>

        <connection>
            <name>test_secure</name>
            <hostname>$TEST_HOST</hostname>
            <secure>1</secure>
        </connection>

        <connection>
            <name>test_database</name>
            <hostname>$TEST_HOST</hostname>
            <database>$CLICKHOUSE_DATABASE</database>
        </connection>

        <connection>
            <name>test_user</name>
            <hostname>$TEST_HOST</hostname>
            <user>MySQL</user>
        </connection>

        <connection>
            <name>test_password</name>
            <hostname>$TEST_HOST</hostname>
            <password>MySQL</password>
        </connection>

        <connection>
            <name>test_history_file</name>
            <hostname>$TEST_HOST</hostname>
            <history_file>/no/such/dir/.history</history_file>
        </connection>
    </connections_credentials>
</clickhouse>
EOL

CONFIG_ROOT_OVERRIDES=$CLICKHOUSE_TMP/client_user_pass.xml
cat > $CONFIG_ROOT_OVERRIDES <<EOL
<clickhouse>
    <host>$TEST_HOST</host>
    <port>$TEST_PORT</port>
    <database>$TEST_DATABASE</database>
    <user>foo</user>
    <password>pass</password>

    <connections_credentials>
        <connection>
            <name>incorrect_auth</name>
            <hostname>$TEST_HOST</hostname>
            <database>system</database>
        </connection>

        <connection>
            <name>default</name>
            <user>default</user>
            <password></password>
            <hostname>$TEST_HOST</hostname>
            <database>system</database>
        </connection>
    </connections_credentials>
</clickhouse>
EOL

echo 'connection'
$CLICKHOUSE_CLIENT --config $CONFIG --connection no_such_connection -q 'select 1' |& grep -F -o "No such connection 'no_such_connection' in connections_credentials"
echo 'hostname'
$CLICKHOUSE_CLIENT --config $CONFIG --host test_hostname_invalid -q 'select 1' |& grep -F -o 'Not found address of host: test_hostname_invalid.'
$CLICKHOUSE_CLIENT --config $CONFIG --connection test_hostname_invalid --host $TEST_HOST -q 'select 1'
$CLICKHOUSE_CLIENT --config $CONFIG -q 'select currentDatabase()'
$CLICKHOUSE_CLIENT --config $CONFIG --host $TEST_HOST -q 'select currentDatabase()'
echo 'port'
$CLICKHOUSE_CLIENT --config $CONFIG --connection test_port -q 'select tcpPort()' |& grep -F -o 'Connection refused (localhost:0).'
$CLICKHOUSE_CLIENT --config $CONFIG --connection test_port --port $TEST_PORT -q 'select tcpPort()'
echo 'secure'

$CLICKHOUSE_CLIENT --config $CONFIG --connection test_secure -q 'select tcpPort()' |& grep -c -F -o -e 'SSL routines::wrong version number' -e 'tcp_secure protocol is disabled because poco library was built without NetSSL support.'

echo 'database'
$CLICKHOUSE_CLIENT --config $CONFIG --connection test_database -q 'select currentDatabase()'
echo 'user'
$CLICKHOUSE_CLIENT --config $CONFIG --connection test_user -q 'select currentUser()' |& grep -F -o 'MySQL: Authentication failed'
$CLICKHOUSE_CLIENT --config $CONFIG --connection test_user --user default -q 'select currentUser()'
echo 'password'
$CLICKHOUSE_CLIENT --config $CONFIG --connection test_password -q 'select currentUser()' |& grep -F -o 'default: Authentication failed: password is incorrect, or there is no user with such name.'
$CLICKHOUSE_CLIENT --config $CONFIG --connection test_password --password "" -q 'select currentUser()'
echo 'history_file'
$CLICKHOUSE_CLIENT --progress off --interactive --config $CONFIG --connection test_history_file -q 'select 1' </dev/null |& grep -F -o 'Cannot create file: /no/such/dir/.history'

# Just in case
unset CLICKHOUSE_USER
unset CLICKHOUSE_PASSWORD
echo 'root overrides'
$CLICKHOUSE_CLIENT --config $CONFIG_ROOT_OVERRIDES --connection incorrect_auth -q 'select currentUser()' |& grep -F -o 'foo: Authentication failed: password is incorrect, or there is no user with such name.'
$CLICKHOUSE_CLIENT --config $CONFIG_ROOT_OVERRIDES --connection incorrect_auth --user "default" --password "" -q 'select currentUser()'
$CLICKHOUSE_CLIENT --config $CONFIG_ROOT_OVERRIDES --connection default -q 'select currentUser()'
$CLICKHOUSE_CLIENT --config $CONFIG_ROOT_OVERRIDES --connection default --user foo -q 'select currentUser()' |& grep -F -o 'foo: Authentication failed: password is incorrect, or there is no user with such name.'

rm -f "${CONFIG:?}"
