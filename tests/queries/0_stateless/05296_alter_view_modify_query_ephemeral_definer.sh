#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: that configuration replaces the user directories, so there is no `memory` storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
author="author_${CLICKHOUSE_DATABASE}"
ephemeral="ephemeral_${CLICKHOUSE_DATABASE}"

# A view whose definer is ephemeral stores a clone of it named `<user>:definer`, and the grant that lets
# the author act as that definer is held on the ephemeral user itself.
${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS $author, $ephemeral, \`$ephemeral:definer\`;
CREATE USER $author IDENTIFIED WITH no_password;
CREATE USER $ephemeral IN memory;

CREATE TABLE $db.src (id UInt64) ENGINE = MergeTree ORDER BY id;
GRANT SELECT ON $db.src TO $author, $ephemeral;

CREATE MATERIALIZED VIEW $db.mv ENGINE = MergeTree ORDER BY id
DEFINER = $ephemeral SQL SECURITY DEFINER
AS SELECT id FROM $db.src;
GRANT ALTER VIEW MODIFY QUERY ON $db.mv TO $author;
"

run() {
    local err
    err=$(${CLICKHOUSE_CLIENT} --user "$author" --query "$2" 2>&1 | grep -o -m1 -E '\([A-Z_]+\)' | tr -d '()')
    echo "$1 ${err:-accepted}"
}

run 'without grant' "ALTER TABLE $db.mv MODIFY QUERY SELECT id FROM $db.src"
${CLICKHOUSE_CLIENT} --query "GRANT SET DEFINER ON $ephemeral TO $author"
run 'with grant   ' "ALTER TABLE $db.mv MODIFY QUERY SELECT id FROM $db.src"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE $db.mv SYNC;
DROP USER IF EXISTS $author, $ephemeral, \`$ephemeral:definer\`;
"
