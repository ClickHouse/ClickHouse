#!/usr/bin/env bash
# An INSERT through an Alias table must charge write accounting once, like a direct INSERT into
# the target: AliasSink forwards the write through a nested InterpreterInsertQuery, which used to
# attach a second CountingTransform over rows the outer INSERT had already counted.
# Settings are pinned per query, not on the client, because clickhouse-test already passes
# --log_comment and a second one is rejected.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

N="${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="u_${N}"
QUOTA_OK="qok_${N}"
QUOTA_LOW="qlow_${N}"
PINS="log_profile_events = 1, async_insert = 0"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${USER};
DROP QUOTA IF EXISTS ${QUOTA_OK};
DROP QUOTA IF EXISTS ${QUOTA_LOW};
CREATE TABLE direct_${N} (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE target_${N} (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE alias_${N} ENGINE = Alias(target_${N});
"

# accounting of one INSERT, by its log_comment
acct() {
    ${CLICKHOUSE_CLIENT} -q "
    SELECT '$1', written_rows, written_bytes,
           ProfileEvents['InsertedRows'], ProfileEvents['InsertedBytes']
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Insert'
      AND current_database = currentDatabase() AND log_comment = '${N}_$1'
    ORDER BY event_time_microseconds DESC LIMIT 1"
}

# 1000 UInt64 rows are 1000 rows / 8000 bytes. Arm 'direct' is the in-range control: it must read
# the same numbers in both directions, which is what makes 'alias' reading twice them a defect.
${CLICKHOUSE_CLIENT} -q "INSERT INTO direct_${N} SELECT number FROM numbers(1000) SETTINGS log_comment = '${N}_direct', ${PINS}"
${CLICKHOUSE_CLIENT} -q "INSERT INTO alias_${N}  SELECT number FROM numbers(1000) SETTINGS log_comment = '${N}_alias', ${PINS}"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
acct direct
acct alias

# The doubling was in the accounting only; both tables hold the rows once.
${CLICKHOUSE_CLIENT} -q "SELECT 'physical', (SELECT count() FROM direct_${N}), (SELECT count() FROM target_${N})"

# A dependent materialized view writes DIFFERENT rows to a DIFFERENT table, so its own insert must
# stay counted: both pairs read 2000/16000 (1000 destination + 1000 view). This arm fails if the
# suppression ever reaches the view insert (1000/8000) as well as if it misses the alias (3000/24000).
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE mvdst_${N} (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE mvsrc_${N} (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_${N} TO mvdst_${N} AS SELECT x FROM mvsrc_${N};
CREATE TABLE mvalias_${N} ENGINE = Alias(mvsrc_${N});
"
${CLICKHOUSE_CLIENT} -q "INSERT INTO mvsrc_${N}   SELECT number FROM numbers(1000) SETTINGS log_comment = '${N}_mv_direct', ${PINS}"
${CLICKHOUSE_CLIENT} -q "INSERT INTO mvalias_${N} SELECT number FROM numbers(1000) SETTINGS log_comment = '${N}_mv_alias', ${PINS}"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
acct mv_direct
acct mv_alias

# The user-visible consequence: a WRITTEN_BYTES quota of 12000 permits the 8000 bytes this insert
# really writes, and the doubled charge refused it outright with QUOTA_EXCEEDED 16000/12000.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE qtarget_${N} (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE qalias_${N} ENGINE = Alias(qtarget_${N});
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER};
CREATE QUOTA ${QUOTA_OK} FOR INTERVAL 100 YEAR MAX WRITTEN BYTES = 12000 TO ${USER};
"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO qalias_${N} SELECT number FROM numbers(1000) SETTINGS ${PINS}"
${CLICKHOUSE_CLIENT} -q "SELECT 'quota_charged', written_bytes FROM system.quotas_usage WHERE quota_name = '${QUOTA_OK}'"
${CLICKHOUSE_CLIENT} -q "SELECT 'quota_rows', count() FROM qtarget_${N}"

# Negative control: a quota the write genuinely exceeds must still reject it. Without this arm, a
# fix that disabled quota accounting for Alias inserts altogether would pass every arm above.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE ntarget_${N} (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE nalias_${N} ENGINE = Alias(ntarget_${N});
DROP QUOTA ${QUOTA_OK};
CREATE QUOTA ${QUOTA_LOW} FOR INTERVAL 100 YEAR MAX WRITTEN BYTES = 4000 TO ${USER};
"
echo -n "quota_too_low "
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO nalias_${N} SELECT number FROM numbers(1000) SETTINGS ${PINS}" 2>&1 | grep -m1 -o QUOTA_EXCEEDED

${CLICKHOUSE_CLIENT} -q "
DROP QUOTA IF EXISTS ${QUOTA_LOW};
DROP USER IF EXISTS ${USER};
"
