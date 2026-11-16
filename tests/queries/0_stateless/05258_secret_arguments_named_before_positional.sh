#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `key = value` argument before the positional ones shifts them: every positional after it is hidden.
mixed_queries=(
    "SELECT * FROM mongodb(foo = 1, '127.0.0.1:27017', 'db', 'coll', 'user', 'leakmongo')"
    "SELECT * FROM redis(foo = 1, '127.0.0.1:6379', 'k', 'k String', 0, 'leakredisf')"
    "SELECT * FROM remote('127.0.0.1', foo = 1, 'db', 't', 'user', 'leakremote')"
    "CREATE TABLE tm1 (x Int32) ENGINE = ArrowFlight(foo = 1, '127.0.0.1:5006', 'ds', 'user', 'leakaf')"
    "CREATE TABLE tm3 (x Int32) ENGINE = ExternalDistributed(foo = 1, 'MySQL', '127.0.0.1:3306', 'db', 't', 'user', 'leakextdist')"
    "CREATE TABLE tm5 (x Int32) ENGINE = YTsaurus(foo = 1, 'http://127.0.0.1', '//p', 'leakyt')"
    "CREATE TABLE tm7 (x Int32) ENGINE = Redis(foo = 1, '127.0.0.1:6379', 0, 'leakredeng', 16) PRIMARY KEY x"
    "SELECT * FROM mysql(foo = 1, '127.0.0.1:3306', 'db', 't', 'user', 'leakmysql')"
)

other_queries=(
    # The all-positional forms hide only the password.
    "SELECT * FROM mongodb('127.0.0.1:27017', 'db', 'coll', 'user', 'pw')"
    "SELECT * FROM redis('127.0.0.1:6379', 'k', 'k String', 0, 'pw')"
    "SELECT * FROM remote('127.0.0.1', 'db', 't', 'user', 'pw')"
    "CREATE TABLE t (x Int32) ENGINE = ArrowFlight('127.0.0.1:5006', 'ds', 'user', 'pw')"
    "CREATE TABLE t (x Int32) ENGINE = ExternalDistributed('MySQL', '127.0.0.1:3306', 'db', 't', 'user', 'pw')"
    "CREATE TABLE t (x Int32) ENGINE = YTsaurus('http://127.0.0.1', '//p', 'pw')"
    "CREATE TABLE t (x Int32) ENGINE = Redis('127.0.0.1:6379', 0, 'pw', 16) PRIMARY KEY x"
    "SELECT * FROM mysql('127.0.0.1:3306', 'db', 't', 'user', 'pw')"
    "SELECT * FROM arrowFlight('127.0.0.1:5006', 'ds', 'user', 'pw')"
    "SELECT * FROM ytsaurus('http://127.0.0.1', '//p', 'pw', 'x Int32')"
    "CREATE DATABASE d ENGINE = MySQL('127.0.0.1:3306', 'db', 'user', 'pw')"
    # A named collection with a key that is an expression hides its value; a positional after a named override is hidden.
    "SELECT * FROM mysql(creds, concat('pass', 'word') = 'S1')"
    "SELECT * FROM mysql(creds, password = 'S2', 'after')"
    "SELECT * FROM postgresql(creds, concat('pass', 'word') = 'S1')"
    "CREATE TABLE t (x Int32) ENGINE = MySQL(creds, concat('pass', 'word') = 'S1')"
    "CREATE TABLE t (x Int32) ENGINE = PostgreSQL(creds, password = 'S2', 'after')"
    "CREATE TABLE t (x Int32) ENGINE = MaterializedPostgreSQL(creds, concat('pass', 'word') = 'S1')"
    "CREATE DATABASE d ENGINE = PostgreSQL(creds, concat('pass', 'word') = 'S1')"
    "CREATE DATABASE d ENGINE = Remote(creds, password = 'S2', 'after')"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB(creds, concat('pass', 'word') = 'S1')"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB(creds, password = 'S2', 'after')"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB(creds, uri = 'mongodb://user:S3@127.0.0.1:27017/db')"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB(creds, collection = 'c', uri = 'mongodb://user:S4@127.0.0.1:27017/db')"
    "SELECT * FROM mongodb('mongodb://user:S3@127.0.0.1:27017/db', 'coll')"
    "CREATE TABLE t (x Int32) ENGINE = Redis(creds, concat('pass', 'word') = 'S1') PRIMARY KEY x"
    "CREATE TABLE t (x Int32) ENGINE = Redis(creds, password = 'S2', 'after') PRIMARY KEY x"
    "SELECT * FROM redis(creds, concat('pass', 'word') = 'S1')"
    "CREATE TABLE t (x Int32) ENGINE = ArrowFlight(creds, concat('pass', 'word') = 'S1')"
    "SELECT * FROM arrowFlight(creds, password = 'S2', 'after')"
    "CREATE TABLE t (x Int32) ENGINE = YTsaurus(creds, concat('oauth', '_token') = 'S1')"
    "SELECT * FROM ytsaurus(creds, oauth_token = 'S2', 'after')"
    "CREATE TABLE t (x Int32) ENGINE = ExternalDistributed('MySQL', creds, concat('pass', 'word') = 'S1')"
    "CREATE TABLE t (x Int32) ENGINE = ExternalDistributed('MySQL', creds, password = 'S2', 'after')"
    "CREATE TABLE t (x Int32) ENGINE = Kafka(creds, concat('kafka_sasl', '_password') = 'S1')"
    "CREATE TABLE t (x Int32) ENGINE = Kafka(creds, kafka_sasl_password = 'S2', 'after')"
    # A url after a named argument is hidden whole, not only its password.
    "SELECT * FROM urlCluster(foo = 1, 'https://user:S5@example.com/path')"
    # A `SETTINGS` clause is not a positional argument and stays readable.
    "SELECT * FROM remote('127.0.0.1', 'db', 't', 'user', 'pw', number = 1, SETTINGS skip_unavailable_shards = 1)"
    "SELECT * FROM mysql(creds, password = 'S2', SETTINGS connect_timeout = 1)"
    # A folded `equals` at the secret slot can hold the secret as its left operand: it is hidden whole.
    "CREATE TABLE t (x Int32) ENGINE = Redis('127.0.0.1:6379', 0, 'S10' = 'x') PRIMARY KEY x"
    "SELECT * FROM redis('127.0.0.1:6379', 'k', 'k String', 0, 'S10' = 'x')"
    "CREATE TABLE t (x Int32) ENGINE = ArrowFlight('127.0.0.1:5006', 'ds', 'user', 'S10' = 'x')"
    "CREATE TABLE t (x Int32) ENGINE = Redis(creds, host = 'h', port = 6379) PRIMARY KEY x"
    "SELECT * FROM redis(localhost, 'k', 'k String', 0, 'S11' = 'x')"
    "CREATE TABLE t (x Int32) ENGINE = ArrowFlight(localhost, 'ds', 'user', 'S11' = 'x')"
    "SELECT * FROM mysql(localhost, 'db', 't', 'u', 'S11' = 'x')"
    "SELECT * FROM mysql(creds, host = 'h', port = 1, db = 'd', table = 't')"
    "CREATE TABLE t (dummy UInt8) ENGINE = Remote(creds, concat('pass', 'word') = 'S12')"
    "SELECT * FROM remote(password = 'S12', '127.0.0.1:9000', 'system', 'one', 'u')"
    "SELECT * FROM remoteSecure(creds, concat('pass', 'word') = 'S12')"
    # An identifier can be a positional endpoint rather than a named collection.
    "SELECT * FROM redis(localhost, 'k', 'k String', 0, 'S6')"
    "SELECT * FROM ytsaurus(proxy, '//p', 'S6', 'x Int32')"
    "CREATE TABLE t (x Int32) ENGINE = YTsaurus(proxy, '//p', 'S6')"
    # Every `uri` override is masked, also beside a `password` one.
    "CREATE TABLE t (x Int32) ENGINE = MongoDB(creds, password = 'S7', uri = 'mongodb://user:S7@127.0.0.1/db')"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB(creds, uri = 'mongodb://user:S8@127.0.0.1/db', uri = 'mongodb://user:S9@127.0.0.1/db')"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB(creds, uri = concat('mongodb://user:', 'S10@127.0.0.1'))"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB('mongodb://user:S11@127.0.0.1/db', 'c', '_id')"
    "SELECT * FROM mongodb('mongodb://user:S11@127.0.0.1/db', 'c', 'x Int32')"
    "SELECT * FROM mongodb('mongodb://user:S11@127.0.0.1/db', 'c', 'x Int32', '_id')"
    "SELECT * FROM mongodb('mongodb://user:S12@127.0.0.1/db', 'c', password = 'S12')"
    "SELECT * FROM mongodb('mongodb://user:S14@127.0.0.1/db', 'c', 'x Int32', '_id', password = 'S14')"
    "SELECT * FROM mongodb()"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB(\`mongodb://user:S15@127.0.0.1/db\`, 'c')"
    "SELECT * FROM mongodb(\`mongodb://user:S15@127.0.0.1/db\`, 'c', 'x Int32')"
    "CREATE TABLE t (x Int32) ENGINE = MongoDB('mongodb://user:S13@127.0.0.1/db?label=it\\'s', 'c')"
)

for query in "${mixed_queries[@]}" "${other_queries[@]}"; do
    $CLICKHOUSE_FORMAT --oneline <<< "$query"
done

# The rejected statements are logged with the same masking.
for query in "${mixed_queries[@]}"; do
    $CLICKHOUSE_CLIENT -q "$query" >/dev/null 2>&1
done
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS tm1; DROP TABLE IF EXISTS tm3; DROP TABLE IF EXISTS tm5; DROP TABLE IF EXISTS tm7"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.query_log
    WHERE current_database = currentDatabase() AND type != 'QueryStart' AND query LIKE '%leak%' AND query NOT LIKE '%query_log%'"
