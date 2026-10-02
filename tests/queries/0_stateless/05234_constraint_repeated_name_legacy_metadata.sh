#!/usr/bin/env bash

# A constraint name declared more than once is rejected when the definition is stated (see
# `05071_alter_constraint_array_join_always_rejected.sql`), but a table stored by a version without
# that check still loads - a replay of stored metadata is deliberately not screened - and each of its
# declarations keeps being enforced. `arrayJoin` in an `ALTER` on such a table used to slip past the
# check of `05069_reject_array_join_in_check_constraint.sql`, because the name the command addressed
# looked free once one declaration of it had been dropped.
#
# The metadata is written by hand here, which is what a table created before the check looks like.

# Creation of a database with the Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/05234_constraint_repeated_name_legacy_metadata"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/stored"

echo "ATTACH DATABASE stored ENGINE = Ordinary" > "${WORKING_FOLDER}/metadata/stored.sql"
cat <<EOF > "${WORKING_FOLDER}/metadata/stored/t.sql"
ATTACH TABLE stored.t (k Int32, arr Array(Int32), CONSTRAINT c CHECK k > 0, CONSTRAINT c CHECK k < 1000) ENGINE = MergeTree ORDER BY tuple();
EOF

# The table attaches: the replay of its metadata is not screened.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "SELECT count() FROM stored.t"

# Each declaration of the repeated name is enforced, and the two are told apart by their expressions.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES (0, [1])" 2>&1 \
    | grep -o -m1 "Expression: (k > 0)"
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES (5000, [1])" 2>&1 \
    | grep -o -m1 "Expression: (k < 1000)"
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES (5, [1]); SELECT count() FROM stored.t"

# The bypass: one declaration of `c` is dropped, so the name the `MODIFY` addresses looked free.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query \
    "ALTER TABLE stored.t DROP CONSTRAINT c, MODIFY CONSTRAINT IF EXISTS c CHECK arrayJoin(arr) > 0" 2>&1 \
    | grep -o -m1 "Constraint \`c\` cannot contain arrayJoin, because it changes the number of rows"

# Nothing was stored, and the `DROP` of that same statement did not take effect either.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query \
    "SELECT create_table_query LIKE '%arrayJoin%', countSubstrings(create_table_query, 'CONSTRAINT c ') FROM system.tables WHERE database = 'stored' AND name = 't'"

# A name that is already stored twice is still addressable, one declaration per `DROP`.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "ALTER TABLE stored.t DROP CONSTRAINT c"
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query \
    "SELECT countSubstrings(create_table_query, 'CONSTRAINT c ') FROM system.tables WHERE database = 'stored' AND name = 't'"
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "ALTER TABLE stored.t DROP CONSTRAINT c"
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query \
    "SELECT create_table_query LIKE '%CONSTRAINT%' FROM system.tables WHERE database = 'stored' AND name = 't'"

rm -rf "${WORKING_FOLDER}"
