#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MySQL integration is not available in the fast test build.

# The TLS credentials of a MySQL source that are given as contents (`ssl_ca_pem`, `ssl_cert_pem`,
# `ssl_key_pem`) must also be redacted in the query-tree surfaces, not only in the formatted query.
# `run_passes = 0` dumps the tree without resolving the table function, so no MySQL server is needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SECRET="SECRET_THAT_MUST_NOT_LEAK"

explain() {
    echo "--- $1"
    local dump
    dump=$($CLICKHOUSE_CLIENT --query "EXPLAIN QUERY TREE run_passes = 0 $2")
    if echo "$dump" | grep -q "$SECRET"; then
        echo "FAIL: secret leaked in the query tree"
    else
        echo "OK"
    fi
    # The name of a masked credential stays visible, and every secret is masked individually: the
    # positional password and both credentials, without hiding the arguments around them.
    echo "$dump" | grep -c "identifier: ssl_ca_pem"
    echo "$dump" | grep -c "constant_value: \[HIDDEN\]"
}

# In the positional form the main secret is the positional password, so the credentials that follow it
# are masked individually: the argument list is not a single named span.
explain "positional arguments" \
    "SELECT * FROM mysql('127.0.0.1:3306', 'db', 't', 'u', '${SECRET}', ssl_ca_pem = '${SECRET}', ssl_key_pem = '${SECRET}')"

explain "named collection" \
    "SELECT * FROM mysql(creds, table = 't', ssl_ca_pem = '${SECRET}', ssl_key_pem = '${SECRET}')"
