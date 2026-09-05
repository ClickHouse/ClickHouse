#!/usr/bin/env bash

# A `CHECK` constraint containing `arrayJoin` is rejected when it is declared (see
# `05069_reject_array_join_in_check_constraint.sql`), but a table stored by a version without that
# check still loads - a replay of stored metadata is deliberately not screened. Inserting into such a
# table used to scan the `arrayJoin`-expanded result and read the block's own columns past their end,
# reported as `Array of size 18446744073709551613 is too large`. Now the size of the result is checked
# against the block, and the constraint is named instead.
#
# The metadata is written by hand here, which is what a table created before the check looks like.

# Creation of a database with the Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/05072_check_constraint_row_count_mismatch"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/stored"

echo "ATTACH DATABASE stored ENGINE = Ordinary" > "${WORKING_FOLDER}/metadata/stored.sql"
cat <<EOF > "${WORKING_FOLDER}/metadata/stored/t.sql"
ATTACH TABLE stored.t (id Int32, arr Array(Int32), CONSTRAINT c CHECK arrayJoin(arr) > 0) ENGINE = MergeTree ORDER BY tuple();
EOF

# The table attaches: the replay of its metadata is not screened.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "SELECT count() FROM stored.t"

# Longer than the block: what used to be read past the end of a block column.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES (1, [1, 1]), (2, [-5])" 2>&1 \
    | grep -o -m1 "Constraint \`c\` for table stored.t returned 3 values for a block of 2 rows"

# Shorter than the block: what used to blame the violation on the wrong row.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES (1, []), (2, [-5])" 2>&1 \
    | grep -o -m1 "Constraint \`c\` for table stored.t returned 1 values for a block of 2 rows"

# Nothing was inserted.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "SELECT count() FROM stored.t"

# Dropping the constraint, as the message suggests, makes the table insertable again.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "ALTER TABLE stored.t DROP CONSTRAINT c"
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES (1, [1, 1]), (2, [-5])"
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "SELECT count() FROM stored.t"

rm -rf "${WORKING_FOLDER}"
