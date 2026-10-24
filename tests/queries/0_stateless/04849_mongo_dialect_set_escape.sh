#!/usr/bin/env bash
# A session must be able to leave the `mongo` dialect after `allow_experimental_mongo_dialect`
# was turned back off: a leading `SET` bypasses the experimental gate, so
# `SET dialect = 'clickhouse'` recovers the session instead of failing with
# `SUPPORT_IS_DISABLED` and stranding it in the dialect until reconnect. A Mongo statement
# stays behind the gate.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS escape;
    CREATE TABLE escape (id Int32) ENGINE = Memory;
    INSERT INTO escape VALUES (1);
"

# One session throughout, because the dialect is a session setting: enter the dialect, run a
# Mongo statement, turn the gate back off, leave the dialect with a `SET`, and run plain SQL
# again. Before the fix the last `SET dialect = 'clickhouse'` failed with `SUPPORT_IS_DISABLED`.
${CLICKHOUSE_CLIENT} --query "
SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';
db.escape.find({});
SET allow_experimental_mongo_dialect = 0;
SET dialect = 'clickhouse';
SELECT id + 1 FROM escape;
"

# A Mongo statement itself stays behind the gate. Only the error text is printed - without
# the `DB::Exception: ` prefix, because the test runner rejects the word `Exception` in the
# standard output of a test - and the server logs are silenced so that a randomized
# `send_logs_level` cannot put a log line in front of the error.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --dialect mongo --query 'db.escape.find({});' 2>&1 >/dev/null \
    | grep -m1 'Code: 344' | grep -o 'Support for the MongoDB dialect is disabled.*' | sed 's/\. (SUPPORT_IS_DISABLED)$//'

${CLICKHOUSE_CLIENT} --query "DROP TABLE escape"
