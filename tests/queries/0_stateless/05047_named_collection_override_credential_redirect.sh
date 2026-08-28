#!/usr/bin/env bash
# Overriding a redirect key must not send a named collection's credentials to another destination.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

pinned="${CLICKHOUSE_TEST_UNIQUE_NAME}_pinned"
no_creds="${CLICKHOUSE_TEST_UNIQUE_NAME}_no_creds"
creds_only="${CLICKHOUSE_TEST_UNIQUE_NAME}_creds_only"
opted_in="${CLICKHOUSE_TEST_UNIQUE_NAME}_opted_in"
pinned_host="${CLICKHOUSE_TEST_UNIQUE_NAME}_pinned_host"

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS $pinned;
DROP NAMED COLLECTION IF EXISTS $no_creds;
DROP NAMED COLLECTION IF EXISTS $creds_only;
DROP NAMED COLLECTION IF EXISTS $opted_in;
DROP NAMED COLLECTION IF EXISTS $pinned_host;

CREATE NAMED COLLECTION $pinned AS
    url = 'http://127.0.0.1:1/bucket/data/',
    access_key_id = 'AKIAIOSFODNN7EXAMPLE',
    secret_access_key = 'secret';

CREATE NAMED COLLECTION $no_creds AS
    url = 'http://127.0.0.1:1/bucket/data/';

CREATE NAMED COLLECTION $creds_only AS
    access_key_id = 'AKIAIOSFODNN7EXAMPLE',
    secret_access_key = 'secret';

CREATE NAMED COLLECTION $opted_in AS
    url = 'http://127.0.0.1:1/bucket/data/' OVERRIDABLE,
    access_key_id = 'AKIAIOSFODNN7EXAMPLE',
    secret_access_key = 'secret';

CREATE NAMED COLLECTION $pinned_host AS
    host = '127.0.0.1',
    port = 9000,
    database = 'system',
    \`table\` = 'one',
    user = 'x',
    password = 'y';
"

# Reports only whether the override itself was refused; whatever happens afterwards is not the point.
# DESCRIBE with an explicit structure resolves the arguments without contacting the destination.
run() {
    if ${CLICKHOUSE_CLIENT} --query "$1" 2>&1 | grep -q "Override not allowed"; then
        echo "REJECTED"
    else
        echo "ALLOWED"
    fi
}

s3_desc() { run "DESCRIBE TABLE s3($1, format='CSV', structure='x UInt8')"; }

echo "-- collection carries credentials and pins a destination"
echo -n "another host: "
s3_desc "$pinned, url='http://127.0.0.2:1/steal/data.csv'"
echo -n "another port: "
s3_desc "$pinned, url='http://127.0.0.1:2/bucket/data.csv'"
echo -n "another path, same destination: "
s3_desc "$pinned, url='http://127.0.0.1:1/bucket/other/data.csv'"
echo -n "key that is not a destination: "
s3_desc "$pinned"

echo "-- nothing to leak, so redirects stay allowed"
echo -n "another host: "
s3_desc "$no_creds, url='http://127.0.0.2:1/steal/data.csv'"

echo "-- collection pins no destination, so the query has to supply one"
echo -n "url from the query: "
s3_desc "$creds_only, url='http://127.0.0.2:1/data/data.csv'"

echo "-- operator opted in with OVERRIDABLE"
echo -n "another host: "
s3_desc "$opted_in, url='http://127.0.0.2:1/steal/data.csv'"

echo "-- aliases of the pinned destination key"
echo -n "host: "
run "DESCRIBE TABLE remote($pinned_host, host='127.0.0.2')"
echo -n "hostname: "
run "DESCRIBE TABLE remote($pinned_host, hostname='127.0.0.2')"
echo -n "addresses_expr: "
run "DESCRIBE TABLE remote($pinned_host, addresses_expr='127.0.0.2:9000')"
echo -n "same host: "
run "DESCRIBE TABLE remote($pinned_host, host='127.0.0.1')"
echo -n "key that is not a destination: "
run "DESCRIBE TABLE remote($pinned_host, database='default')"
# A port selects a service on a host the operator already chose, so it stays overridable.
echo -n "port: "
run "DESCRIBE TABLE remote($pinned_host, port=9001)"

echo "-- dictionary source, which takes its overrides from the DDL instead of function arguments"
# The source configuration is resolved when the dictionary is loaded, not when it is created.
dict_load() {
    ${CLICKHOUSE_CLIENT} --query "CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.$1 (k UInt64, v String)
        PRIMARY KEY k LAYOUT(FLAT) LIFETIME(0)
        SOURCE(CLICKHOUSE(NAME $pinned_host HOST '$2'))" >/dev/null 2>&1
    run "SELECT dictGet('${CLICKHOUSE_DATABASE}.$1', 'v', toUInt64(1))"
}
echo -n "another host: "
dict_load d_redirect '127.0.0.2'
echo -n "same host: "
dict_load d_same '127.0.0.1'

${CLICKHOUSE_CLIENT} -m --query "
DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.d_redirect;
DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.d_same;
DROP NAMED COLLECTION $pinned;
DROP NAMED COLLECTION $no_creds;
DROP NAMED COLLECTION $creds_only;
DROP NAMED COLLECTION $opted_in;
DROP NAMED COLLECTION $pinned_host;
"
