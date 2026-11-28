#!/usr/bin/env bash
# Tags: long, no-debug, no-fasttest
# no-fasttest: the fast-test server runs without a query log, so there is no history to seed from.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The predictive autocomplete model seeds from `system.user_query_log`, which shows each user only
# their own query log records and requires no grants. Prepare a fresh user with zero grants and a
# single distinctive query in their history, then check (from a PTY, in the python part) that a new
# interactive session as that user predicts the next token from that history.

TEST_USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $TEST_USER"
$CLICKHOUSE_CLIENT --query "CREATE USER $TEST_USER IDENTIFIED WITH no_password"

# The history to seed from: a successfully finished initial query by the test user. It needs no
# grants (no table is read). `anyHeavy` is distinctive enough that only a prediction learned from
# this history can produce it right after `SELECT `.
$CLICKHOUSE_CLIENT --user "$TEST_USER" --query "SELECT anyHeavy(1)" > /dev/null

# The seeding query reads the query log table backing `system.user_query_log`; make sure the
# history query above has been flushed into it.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

TEST_USER="$TEST_USER" python3 "$CUR_DIR"/04838_clickhouse_client_autocomplete_model_history_seed.python

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $TEST_USER"
