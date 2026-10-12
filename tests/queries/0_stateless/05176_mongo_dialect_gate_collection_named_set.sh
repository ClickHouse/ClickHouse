#!/usr/bin/env bash
# The `SET` escape of the experimental gate of the `mongo` dialect uses the same `ParserSetQuery`
# probe as the Mongo parser, so a statement escapes the gate exactly when the parser also reads it
# as a SQL `SET`. A Mongo statement whose database part is named `set` starts with the word `set`
# as well, and before the fix that first token alone let it run with
# `allow_experimental_mongo_dialect = 0`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With the gate off, `set.users.find({})` is a Mongo statement and is rejected. Before the fix it
# was taken for a SQL `SET`, skipped the gate and was executed as a Mongo query, which failed
# with `UNKNOWN_DATABASE` instead. Only the error text is printed - without the `DB::Exception: `
# prefix, because the test runner rejects the word `Exception` in the standard output of a test -
# and the server logs are silenced so that a randomized `send_logs_level` cannot put a log line
# in front of the error.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --dialect mongo --query 'set.users.find({});' 2>&1 >/dev/null \
    | grep -m1 -o 'Code: [0-9]*'

# With the gate on it is parsed as a Mongo statement: the database `set` does not exist, which is
# `UNKNOWN_DATABASE` (81) rather than a silently executed `SET`.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_experimental_mongo_dialect 1 --dialect mongo \
    --query 'set.users.find({});' 2>&1 >/dev/null | grep -m1 -o 'Code: [0-9]*'

# A real SQL `SET` still escapes the gate, so a session can always leave the dialect.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "
SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';
SET allow_experimental_mongo_dialect = 0;
SET dialect = 'clickhouse';
SELECT 'left the dialect';
"
