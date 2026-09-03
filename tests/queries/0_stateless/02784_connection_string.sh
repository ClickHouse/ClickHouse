#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_INFOS=('default' '')
HOSTS_PORTS=("$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP" "$CLICKHOUSE_HOST" "$CLICKHOUSE_HOST:" ":$CLICKHOUSE_PORT_TCP"  "127.0.0.1" "127.0.0.1:$CLICKHOUSE_PORT_TCP" "$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP,invalid_host:9000" "[0000:0000:0000:0000:0000:0000:0000:0001]" "[::1]"  "[::1]:$CLICKHOUSE_PORT_TCP" "" )
DATABASES=("$CLICKHOUSE_DATABASE" "")

TEST_INDEX=0

function runClient()
{
    $CLICKHOUSE_CLIENT_BINARY "$@" -q "SELECT $TEST_INDEX" --log_comment 02766_connection_string.sh --send_logs_level=warning
    ((++TEST_INDEX))
}

function testConnectionString()
{
    if [ "$database" == "" ]; then
        runClient "clickhouse:$1"
        runClient "clickhouse:$1/"
    else
        runClient "clickhouse:$1/$database"
    fi
}

function testConnectionWithUserName()
{
if [ "$user_info" == "" ] && [ "$host_port" == "" ]; then
        testConnectionString "//"
        testConnectionString ""
    else
        testConnectionString "//$user_info@$host_port"
    fi
}

for user_info in "${USER_INFOS[@]}"
do
    for host_port in "${HOSTS_PORTS[@]}"
    do
        for database in "${DATABASES[@]}"
        do
            testConnectionWithUserName
        done
    done
done

# Specific user and password
TEST_INDEX=500
TEST_USER_NAME="test_user_02771_$$"
TEST_USER_EMAIL_NAME="test_user_02771_$$@some_mail.com"
TEST_USER_EMAIL_NAME_ENCODED="test_user_02771_$$%40some_mail.com"

TEST_USER_PASSWORD="zyx%$&abc"
# %, $, & percent encoded
TEST_USER_PASSWORD_ENCODED="zyx%25%24%26abc"

$CLICKHOUSE_CLIENT -q "CREATE USER '$TEST_USER_NAME'"
$CLICKHOUSE_CLIENT -q "CREATE USER '$TEST_USER_EMAIL_NAME' IDENTIFIED WITH plaintext_password BY '$TEST_USER_PASSWORD'"

runClient "clickhouse://$TEST_USER_NAME@$CLICKHOUSE_HOST/$CLICKHOUSE_DATABASE"
runClient "clickhouse://$TEST_USER_EMAIL_NAME_ENCODED:$TEST_USER_PASSWORD_ENCODED@$CLICKHOUSE_HOST/$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP USER '$TEST_USER_NAME'"
$CLICKHOUSE_CLIENT -q "DROP USER '$TEST_USER_EMAIL_NAME'"

# Percent-encoded database in non-ascii symbols
UTF8_DATABASE="БазаДанных_$$"
UTF8_DATABASE_PERCENT_ENCODED="%D0%91%D0%B0%D0%B7%D0%B0%D0%94%D0%B0%D0%BD%D0%BD%D1%8B%D1%85_$$"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE IF NOT EXISTS \`$UTF8_DATABASE\`"
runClient "clickhouse://default@$CLICKHOUSE_HOST/$UTF8_DATABASE_PERCENT_ENCODED"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS \`$UTF8_DATABASE\`"

# clickhouse-client extra options cases
TEST_INDEX=1000

runClient "clickhouse://$CLICKHOUSE_HOST/" --user 'default'
runClient "clickhouse://$CLICKHOUSE_HOST/default" --user 'default'
runClient "clickhouse:" --database "$CLICKHOUSE_DATABASE"

# User 'default' and default host
runClient "clickhouse://default@"

# Invalid URI cases
TEST_INDEX=10000
runClient "clickhouse://default:@$CLICKHOUSE_HOST/" --user 'default' 2>&1 | grep -o 'Bad arguments'
runClient "clickhouse://default:pswrd@$CLICKHOUSE_HOST/" --user 'default' 2>&1 | grep -o 'Bad arguments'
runClient "clickhouse://default:pswrd@$CLICKHOUSE_HOST/" --password 'pswrd' 2>&1 | grep -o 'Bad arguments'
runClient "clickhouse:///$CLICKHOUSE_DATABASE" --database "$CLICKHOUSE_DATABASE" 2>&1 | grep -o 'Bad arguments'
runClient "clickhouse://$CLICKHOUSE_HOST/$CLICKHOUSE_DATABASE" --database "$CLICKHOUSE_DATABASE" 2>&1 | grep -o 'Bad arguments'
runClient "clickhouse://$CLICKHOUSE_HOST/$CLICKHOUSE_DATABASE?s" --database "$CLICKHOUSE_DATABASE" 2>&1 | grep -o 'Bad arguments'
runClient "clickhouse:/$CLICKHOUSE_DATABASE?s" --database "$CLICKHOUSE_DATABASE" 2>&1 | grep -o 'Bad arguments'

runClient "http://" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "click_house:" 2>&1 | grep -o 'BAD_ARGUMENTS'

TEST_INDEX=1000087
# Using connection string prohibits to use --host and --port options
runClient "clickhouse://default:@$CLICKHOUSE_HOST/" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://default:@$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP/" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://default:@$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP/" --port "$CLICKHOUSE_PORT_TCP" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://default:@$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP/" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://default:@$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP/" --host "$CLICKHOUSE_HOST" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://default:@$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP/" --port "$CLICKHOUSE_PORT_TCP" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://default:@$CLICKHOUSE_HOST/" --port "$CLICKHOUSE_PORT_TCP" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://$CLICKHOUSE_HOST/" --port "$CLICKHOUSE_PORT_TCP" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://:@$CLICKHOUSE_HOST/" --port "$CLICKHOUSE_PORT_TCP" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://$CLICKHOUSE_HOST/" --port "$CLICKHOUSE_PORT_TCP" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:" --port "$CLICKHOUSE_PORT_TCP" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://" --port "$CLICKHOUSE_PORT_TCP" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:///" --port "$CLICKHOUSE_PORT_TCP" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:///?" --port "$CLICKHOUSE_PORT_TCP" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://:/?" --port "$CLICKHOUSE_PORT_TCP" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:" --database "$CLICKHOUSE_DATABASE" --port "$CLICKHOUSE_PORT_TCP" --host "$CLICKHOUSE_HOST" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Using clickhouse-client and connection is prohibited
runClient "clickhouse:" --connection "connection" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Space is used in connection string (This is prohibited).
runClient " clickhouse:" 2>&1 | grep -o 'SYNTAX_ERROR'
runClient "clickhouse: " 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://host1 /" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://host1, host2/" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://host1 ,host2/" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://host1 host2/" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://host1/ database:" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://user :password@host1" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://user: password@host1" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Connection string is not first argument
runClient --multiline "clickhouse://default:@$CLICKHOUSE_HOST/" 2>&1 | grep -o 'BAD_ARGUMENTS'
# Connection string used as the first and the second argument of client
runClient "clickhouse://default:@$CLICKHOUSE_HOST/" "clickhouse://default:@$CLICKHOUSE_HOST/" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Invalid hosts
runClient "clickhouse://host1,,," 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse://," 2>&1 | grep -o 'BAD_ARGUMENTS'

# Invalid parameters
runClient "clickhouse:?invalid_parameter" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:?invalid_parameter&secure" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:?s&invalid_parameter" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:?s&invalid_parameter=val" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:?invalid_parameter=arg" 2>&1 | grep -o 'BAD_ARGUMENTS'
runClient "clickhouse:?invalid_parameter=arg&s" 2>&1 | grep -o 'BAD_ARGUMENTS'
# Several users prohibited
runClient "clickhouse://user1@localhost,default@localhost/" 2>&1 | grep -o 'BAD_ARGUMENTS'
# Using '@' in user name is prohibited. User name should be percent-encoded.
runClient "clickhouse://my_mail@email.com@host/" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Wrong input cases
TEST_INDEX=100000
# Invalid user name
runClient "clickhouse://non_exist_user@$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP/" 2>&1 | grep -o 'Authentication failed'
# Invalid password
runClient "clickhouse://default:invalid_password@$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP/" 2>&1 | grep -o 'Authentication failed'
