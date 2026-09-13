#!/usr/bin/env bash

# A constraint containing `arrayJoin` is rejected when it is declared (see
# `05069_reject_array_join_in_check_constraint.sql`), but a table stored by a version without that
# check still loads. Such a stored constraint must not be trusted by the query-time constraint
# optimizer either: it holds per expanded row, not per stored row, so matching it against
# `WHERE arrayJoin(arr) > 0` and removing the filter would answer a query over the exploded rows with
# the base rows.
#
# The metadata is written by hand here, which is what a table created before the check looks like:
# the rows are inserted first, and the constraint is put into the stored definition afterwards.

# Creation of a database with the Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/05182_legacy_array_join_constraint_not_used_by_optimizer"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/stored"

echo "ATTACH DATABASE stored ENGINE = Ordinary" > "${WORKING_FOLDER}/metadata/stored.sql"

for constraint_type in CHECK ASSUME
do
    echo "ATTACH TABLE stored.t (id Int32, arr Array(Int32)) ENGINE = MergeTree ORDER BY tuple();" \
        > "${WORKING_FOLDER}/metadata/stored/t.sql"
    ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES (1, [1, -1]), (2, [-2, -2])"

    echo "ATTACH TABLE stored.t (id Int32, arr Array(Int32), CONSTRAINT c ${constraint_type} arrayJoin(arr) > 0) ENGINE = MergeTree ORDER BY tuple();" \
        > "${WORKING_FOLDER}/metadata/stored/t.sql"

    for analyzer in 0 1
    do
        echo "--- ${constraint_type}, enable_analyzer = ${analyzer}"
        ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "
            SELECT arrayJoin(arr) AS e FROM stored.t WHERE arrayJoin(arr) > 0 ORDER BY e
            SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1,
                     optimize_append_index = 1, enable_analyzer = ${analyzer}"
    done

    rm -rf "${WORKING_FOLDER}/data" "${WORKING_FOLDER}/store" "${WORKING_FOLDER}/metadata/stored/t.sql"
done

rm -rf "${WORKING_FOLDER}"
