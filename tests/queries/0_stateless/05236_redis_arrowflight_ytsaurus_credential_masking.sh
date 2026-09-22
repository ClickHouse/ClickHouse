#!/usr/bin/env bash

# A named-collection override whose key the parser evaluates but the secret-arguments finder can only
# read as a plain literal must still have its value hidden in `system.query_log`. A collection this test
# creates lets the `Redis` arms with a readable or computed string key be accepted and really apply the
# override; the remaining arms are positional, use a collection that does not exist, or are rejected
# outright, because masking runs when the statement is formatted for logging, before it is validated.
# Every credential is a distinct `leak05236*` canary, so a leak points straight at the site that leaked
# it, and each positive case is paired with the control that has to stay fully visible.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The name must start with a letter, or the formatter backticks it and the substitution below has to
# cope with two spellings of the same collection.
COLL="c${CLICKHOUSE_TEST_UNIQUE_NAME}"
# `CLICKHOUSE_TEST_UNIQUE_NAME` is only the test name and `CLICKHOUSE_DATABASE`, and that database is
# pinned whenever the runner is given `--database`, so without a run-local part in the query ids the
# reads below can be answered by an earlier run's rows instead of this one's.
PREFIX="05236_${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}${RANDOM}"
ARMS=0

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS $COLL"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION $COLL AS
    host = '127.0.0.1', port = 6379, db_index = 0, password = 'leak05236fromcollection', pool_size = 1"

arm() { # arm <name> <statement>
    ARMS=$((ARMS + 1))
    $CLICKHOUSE_CLIENT --query_id="${PREFIX}_$1" --log_queries=1 -q "$2" > /dev/null 2>&1
}

# `StorageRedis` reads `password` from the collection, and the collection parser EVALUATES an override's
# key, so a computed string key names `password` just as a literal one does: `a1` is accepted and the
# credential hidden there is one in active use. A numeric key is not a String, so `a2` is rejected by
# `checkAndGetLiteralArgument<String>`; its value must still be hidden, because the statement is
# formatted for the log before it is validated.
arm a1_redis_computed_key "CREATE TABLE t05236a (k String, v String) ENGINE = Redis($COLL, concat('pass', 'word') = 'leak05236computed') PRIMARY KEY k"
arm a2_redis_numeric_key  "CREATE TABLE t05236b (k String, v String) ENGINE = Redis($COLL, 0 = 'leak05236numeric') PRIMARY KEY k"
# Every override is applied in turn, so a readable credential key is not a reason to stop looking.
arm a3_redis_readable_then_computed "CREATE TABLE t05236c (k String, v String) ENGINE = Redis($COLL, password = 'leak05236readable', concat('pass', 'word') = 'leak05236second') PRIMARY KEY k"
# A readable non-secret override stays fully visible, and so does the positional form.
arm a4_redis_nonsecret_control "CREATE TABLE t05236d (k String, v String) ENGINE = Redis($COLL, pool_size = 7) PRIMARY KEY k"
arm a5_redis_positional        "CREATE TABLE t05236e (k String, v String) ENGINE = Redis('127.0.0.1:6379', 0, 'leak05236positional') PRIMARY KEY k"
# `StorageRedis` folds every positional argument through `evaluateConstantExpressionOrIdentifierAsLiteral`,
# so `0 = 1` is a legal db_index and the password stays at argument 2. Masking the argument whole also
# covers a secret written as the LEFT operand of an `equals` at that slot.
arm a6_redis_folded_positional  "CREATE TABLE t05236f (k String, v String) ENGINE = Redis('127.0.0.1:6379', 0 = 1, 'leak05236folded', 16) PRIMARY KEY k"
arm a7_redis_left_operand       "CREATE TABLE t05236g (k String, v String) ENGINE = Redis('127.0.0.1:6379', 0, 'leak05236leftoperand' = 'x') PRIMARY KEY k"

# One finder serves the `ArrowFlight` engine, `arrowFlight` and the obsolete `arrowflight` alias, so
# each dispatched name is its own arm.
arm b1_arrowflight_engine      "CREATE TABLE t05236h (x UInt8) ENGINE = ArrowFlight(nc05236, concat('pass', 'word') = 'leak05236afengine')"
arm b2_arrowflight_function    "SELECT * FROM arrowFlight(nc05236, concat('pass', 'word') = 'leak05236affunction')"
arm b3_arrowflight_alias       "SELECT * FROM arrowflight(nc05236, concat('pass', 'word') = 'leak05236afalias')"
arm b4_arrowflight_nonsecret_control "SELECT * FROM arrowFlight(nc05236, dataset = 'ds05236')"
arm b5_arrowflight_positional  "SELECT * FROM arrowFlight('h05236:5006', 'ds05236', 'usr05236', 'leak05236afpositional')"

# `StorageYTsaurus` reads `oauth_token` from a collection, which this finder never scanned for, so even
# a plain readable key leaked. Both dispatched names are arms.
arm c1_ytsaurus_engine_plain_key    "CREATE TABLE t05236i (x UInt8) ENGINE = YTsaurus(nc05236, oauth_token = 'leak05236ytplain')"
arm c2_ytsaurus_function_plain_key  "SELECT * FROM ytsaurus(nc05236, oauth_token = 'leak05236ytfnplain')"
arm c3_ytsaurus_engine_computed_key "CREATE TABLE t05236j (x UInt8) ENGINE = YTsaurus(nc05236, concat('oauth', '_token') = 'leak05236ytcomputed')"
arm c4_ytsaurus_nonsecret_control   "CREATE TABLE t05236k (x UInt8) ENGINE = YTsaurus(nc05236, cypress_path = '//tbl05236')"
# Argument 2 carries the token in the positional form (`c5`), and an identifier at argument 0 does not
# make it safe (`c6`): that call is rejected, but it is formatted for the log first, so the token must
# still be hidden (fail closed).
arm c5_ytsaurus_positional          "CREATE TABLE t05236l (x UInt8) ENGINE = YTsaurus('http://proxy05236', '//tbl05236', 'leak05236ytpositional')"
arm c6_ytsaurus_identifier_first    "CREATE TABLE t05236m (x UInt8) ENGINE = YTsaurus(nc05236, 'a05236', 'leak05236ytident')"

# The rule and its helper are shared with the carriers that already fail closed, so these must agree.
arm d1_s3_computed_key    "SELECT * FROM s3(nc05236, concat('secret_access', '_key') = 'leak05236s3')"
arm d2_mysql_computed_key "SELECT * FROM mysql(nc05236, concat('ssl_ca', '_pem') = 'leak05236mysql', table = 't05236')"

for _ in {1..120}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    LOGGED=$($CLICKHOUSE_CLIENT -q "SELECT uniqExact(query_id) FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id LIKE '${PREFIX}\_%'")
    [ "$LOGGED" -ge "$ARMS" ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "SELECT replaceOne(query_id, '${PREFIX}_', '') AS arm,
        replaceAll(max(query), '$COLL', 'nc05236')
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id LIKE '${PREFIX}\_%'
    GROUP BY arm ORDER BY arm FORMAT TSVRaw"

# No canary in any column that prints the statement or its settings, in any arm.
$CLICKHOUSE_CLIENT -q "SELECT countIf(position(concat(query, formatted_query, toString(Settings)), 'leak05236') > 0)
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id LIKE '${PREFIX}\_%'"

# The tables the accepted arms created still reference the collection, so they go first.
for T in a b c d e f g h i j k l m; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t05236$T"
done
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS $COLL"
