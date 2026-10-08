#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The author is stored inside the mutation entry, so its length is capped. A user name that does not
# fit must be rejected explicitly instead of being truncated, which would record two distinct users
# under the same `author`.
# User names are global, so they are suffixed with the test database to let the test run in parallel
# with itself.
LONG_USER="$(printf 'u%.0s' {1..250})_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS t_mutation_author_long_user_name;
DROP USER IF EXISTS \`$LONG_USER\`;

CREATE TABLE t_mutation_author_long_user_name (id UInt64, value String) ENGINE = MergeTree ORDER BY id
SETTINGS persist_mutation_author = 1;
INSERT INTO t_mutation_author_long_user_name VALUES (1, 'a');

CREATE USER \`$LONG_USER\` IDENTIFIED WITH no_password;
GRANT ALTER UPDATE ON *.* TO \`$LONG_USER\`;
"

# The name is longer than the supported 256 bytes, so the mutation is refused.
$CLICKHOUSE_CLIENT --query "
EXECUTE AS \`$LONG_USER\` ALTER TABLE t_mutation_author_long_user_name UPDATE value = 'b' WHERE id = 1 SETTINGS mutations_sync = 1;
" 2>&1 | grep -oF 'TOO_LARGE_STRING_SIZE' | head -n 1

$CLICKHOUSE_CLIENT --multiquery "
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation_author_long_user_name';

DROP TABLE t_mutation_author_long_user_name;
DROP USER \`$LONG_USER\`;
"
