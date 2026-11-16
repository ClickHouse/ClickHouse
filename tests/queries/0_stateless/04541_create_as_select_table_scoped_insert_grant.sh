#!/usr/bin/env bash
# Regression test for the temporary-table publish path of `CREATE TABLE ... AS SELECT` on Atomic
# databases (issue https://github.com/ClickHouse/ClickHouse/issues/26746).
#
# The populating INSERT SELECT runs into an internal `_tmp_replace_*` table, so the target-`INSERT`
# access check must not be authorized against that random name -- otherwise a user with a table-scoped
# `INSERT ON db.dst` grant (as opposed to a wildcard `INSERT ON db.*`) would get a spurious
# `ACCESS_DENIED` on the temporary name, regressing the pre-existing contract where `CREATE TABLE` +
# `INSERT ON db.dst` + `SELECT` on the sources was sufficient. The fix authorizes `INSERT` on the final
# name up front (as the user) and skips only the redundant target check on the temporary name; the source
# `SELECT` access is still enforced. This test proves the table-scoped grant now succeeds, while a user
# lacking `INSERT` on the target is still denied (and leaves no orphan table behind).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

granted="granted_${CLICKHOUSE_TEST_UNIQUE_NAME}"
nogrant="nogrant_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${granted}, ${nogrant};
CREATE TABLE src (x Int) ENGINE = Memory;
INSERT INTO src VALUES (1), (2), (3);
CREATE USER ${granted} IDENTIFIED WITH plaintext_password BY '${granted}';
CREATE USER ${nogrant} IDENTIFIED WITH plaintext_password BY '${nogrant}';
GRANT TABLE ENGINE ON Memory TO ${granted}, ${nogrant};
GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${granted}, ${nogrant};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src TO ${granted}, ${nogrant};
-- A *table-scoped* INSERT grant on the final destination only (not a wildcard INSERT ON db.*).
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dst TO ${granted};
"

granted_client=(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --user "${granted}" --password "${granted}")
nogrant_client=(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --user "${nogrant}" --password "${nogrant}")

echo "-- [table-scoped INSERT grant] CREATE ... AS SELECT into the granted table must succeed:"
create_output=$("${granted_client[@]}" --query "CREATE TABLE dst ENGINE = Memory AS SELECT x FROM src ORDER BY x" 2>&1)
create_status=$?
if [ "${create_status}" -eq 0 ]; then echo "succeeded"; else echo "FAILED (exit ${create_status}): ${create_output}"; fi
echo "-- [table-scoped INSERT grant] and the table is populated:"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dst ORDER BY x"

echo "-- [no INSERT grant] CREATE ... AS SELECT into a table the user cannot INSERT into must be denied:"
"${nogrant_client[@]}" --query "CREATE TABLE dst_denied ENGINE = Memory AS SELECT x FROM src" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
echo "-- [no INSERT grant] and the denied query must not leave an orphan table:"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE dst_denied"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS dst;
DROP TABLE src;
DROP USER ${granted}, ${nogrant};
"
