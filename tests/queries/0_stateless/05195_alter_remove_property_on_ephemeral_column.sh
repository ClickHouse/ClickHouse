#!/usr/bin/env bash
# `REMOVE DEFAULT` on an `EPHEMERAL` column advised `Use REMOVE EPHEMERAL`, which `ParserAlterQuery`
# does not accept, so the advice led to a syntax error. The table below is the reporter's own repro.
# `TestHint` can only match error codes, and the code does not change here, so this has to be a shell
# test asserting the text.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under a `Replicated` database every DDL statement also prints a row of queue status, which the
# reference must not depend on. Silencing it keeps the test running there instead of excluding it
# with `no-replicated-database`; the error messages asserted below are identical either way.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_ephemeral_remove;
CREATE TABLE t_ephemeral_remove (id UInt64, value UInt64 EPHEMERAL id) ENGINE = MergeTree ORDER BY id;
"

for property in DEFAULT MATERIALIZED ALIAS
do
    expected="Cannot remove ${property} from column \`value\`, because column default type is EPHEMERAL."
    expected="${expected} EPHEMERAL is not removable as a property: use DROP COLUMN \`value\` to delete"
    expected="${expected} the column, or MODIFY COLUMN to replace it with a DEFAULT or MATERIALIZED expression"

    echo "-- REMOVE ${property} on an EPHEMERAL column"
    error=$(${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_ephemeral_remove MODIFY COLUMN value REMOVE ${property}" 2>&1)
    # `uniq` because the suite runs with `--send_logs_level=warning`, so the server streams its own
    # log of the exception to the client and the text arrives twice.
    echo "${error}" | grep -oF "${expected}" | uniq
    # Asserted apart from the sentence above so that it survives any rewording of the advice: whatever
    # the message says, it must not send the reader to a syntax `ParserAlterQuery` rejects.
    echo "names REMOVE EPHEMERAL: $(echo "${error}" | grep -cF 'REMOVE EPHEMERAL')"
done

# Both operations the new advice names have to work on an `EPHEMERAL` column, or the advice is no
# better than the old one. Each starts from the ephemeral state, so neither is proven by the other.
echo "-- DROP COLUMN on an EPHEMERAL column, then columns left named value"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_ephemeral_remove DROP COLUMN value"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_ephemeral_remove' AND name = 'value'"

echo "-- MODIFY COLUMN replacing EPHEMERAL, then its default_kind"
${CLICKHOUSE_CLIENT} -q "
DROP TABLE t_ephemeral_remove;
CREATE TABLE t_ephemeral_remove (id UInt64, value UInt64 EPHEMERAL id) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_ephemeral_remove MODIFY COLUMN value UInt64 DEFAULT 7;
SELECT default_kind FROM system.columns WHERE database = currentDatabase() AND table = 't_ephemeral_remove' AND name = 'value';
DROP TABLE t_ephemeral_remove;
"

# the three kinds `REMOVE` does accept: their advice is right, and the fix must leave it verbatim
echo "-- the three kinds REMOVE accepts keep their advice"
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_other_kinds_remove;
CREATE TABLE t_other_kinds_remove
(
    def UInt64 DEFAULT 42,
    mat UInt64 MATERIALIZED def * def,
    ali UInt64 ALIAS def + 1
)
ENGINE = MergeTree ORDER BY tuple();
"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_other_kinds_remove MODIFY COLUMN def REMOVE MATERIALIZED" 2>&1 | grep -oF 'Use REMOVE DEFAULT to delete it' | uniq
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_other_kinds_remove MODIFY COLUMN mat REMOVE ALIAS" 2>&1 | grep -oF 'Use REMOVE MATERIALIZED to delete it' | uniq
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_other_kinds_remove MODIFY COLUMN ali REMOVE DEFAULT" 2>&1 | grep -oF 'Use REMOVE ALIAS to delete it' | uniq
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_other_kinds_remove"
