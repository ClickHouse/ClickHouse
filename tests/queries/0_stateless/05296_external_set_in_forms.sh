#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-in-forms.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# Every form of `IN` whose set is filled while the query runs: a subquery or a table, in each clause and kind
# of query, under both analyzers, and the set of the conversion of `JOIN` to `IN`. Each has its own
# `log_comment`.
QUERIES=$(cat <<'SQL'
CREATE TABLE keys (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO keys SELECT number * 3 FROM numbers(10000);
CREATE TABLE keys_memory (k UInt64) ENGINE = Memory;
INSERT INTO keys_memory SELECT number * 3 FROM numbers(10000);

SELECT 'table', countIf(number IN keys), countIf(number IN keys_memory), countIf(number NOT IN keys) FROM numbers(30000)
SETTINGS log_comment = 'table';
SELECT 'global', countIf(number GLOBAL IN (SELECT number * 3 FROM numbers(10000))),
    countIf(number GLOBAL NOT IN (SELECT number * 3 FROM numbers(10000))) FROM numbers(30000)
SETTINGS log_comment = 'global';
SELECT 'projection', sum(cityHash64(number) * (number IN (SELECT number * 3 FROM numbers(10000)))) FROM numbers(30000)
SETTINGS log_comment = 'projection';
SELECT 'prewhere', count() FROM keys PREWHERE k IN (SELECT number * 6 FROM numbers(10000)) SETTINGS log_comment = 'prewhere';
SELECT 'having', count() FROM (SELECT number % 1000 AS g FROM numbers(30000) GROUP BY g HAVING g IN (SELECT number * 3 FROM numbers(100)))
SETTINGS log_comment = 'having';
SELECT 'lambda', arrayCount(x -> x IN (SELECT number * 3 FROM numbers(10000)), range(30000)) SETTINGS log_comment = 'lambda';
SELECT 'join', count() FROM numbers(30000) AS l JOIN keys AS r ON l.number = r.k WHERE l.number IN (SELECT number * 2 FROM numbers(10000))
SETTINGS log_comment = 'join';
SELECT 'join on', count() FROM numbers(30000) AS l JOIN keys AS r ON l.number = r.k AND l.number IN (SELECT number * 2 FROM numbers(10000))
SETTINGS log_comment = 'join on';
SELECT 'merge', count() FROM merge(currentDatabase(), '^keys$') WHERE k IN (SELECT number * 6 FROM numbers(10000))
SETTINGS log_comment = 'merge';

CREATE VIEW v AS SELECT number FROM numbers(30000) WHERE number IN (SELECT number * 3 FROM numbers(10000));
SELECT 'view', count() FROM v SETTINGS log_comment = 'view';

CREATE TABLE target (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO target SETTINGS log_comment = 'insert' SELECT number FROM numbers(30000) WHERE number IN (SELECT number * 5 FROM numbers(10000));
SELECT 'insert', count() FROM target;

CREATE TABLE source (k UInt64) ENGINE = Null;
CREATE TABLE mv_target (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv TO mv_target AS SELECT k FROM source WHERE k IN (SELECT number * 3 FROM numbers(10000));
INSERT INTO source SETTINGS log_comment = 'materialized view' SELECT number FROM numbers(30000);
SELECT 'materialized view', count() FROM mv_target;

-- The refresh runs as a query of its own, with the settings of the view.
CREATE DATABASE atomic ENGINE = Atomic;
CREATE MATERIALIZED VIEW atomic.rmv REFRESH EVERY 1 YEAR ENGINE = Memory
AS SELECT count() AS c FROM numbers(30000) WHERE number IN (SELECT number * 3 FROM numbers(10000))
SETTINGS log_comment = 'refreshable materialized view';
SYSTEM WAIT VIEW atomic.rmv;
SELECT 'refreshable materialized view', c FROM atomic.rmv;

-- The dictionary runs its source query while the query that needs it waits.
CREATE DICTIONARY dict (k UInt64, v UInt64) PRIMARY KEY k
SOURCE(CLICKHOUSE(QUERY 'SELECT number AS k, number AS v FROM numbers(30000) WHERE number IN (SELECT number * 3 FROM numbers(10000))'))
LAYOUT(HASHED()) LIFETIME(0);
SELECT 'dictionary', dictGetOrDefault('dict', 'v', toUInt64(9), 0), dictGetOrDefault('dict', 'v', toUInt64(10), 0)
SETTINGS log_comment = 'dictionary';

SELECT 'old analyzer', countIf(number IN (SELECT number * 3 FROM numbers(10000))), countIf(number IN keys) FROM numbers(30000)
SETTINGS enable_analyzer = 0, log_comment = 'old analyzer';

-- The conversion of `JOIN` to `IN` replaces the hash join by a set.
SELECT 'join to in', count(), sum(l.number) FROM numbers(1000) AS l ANY INNER JOIN (SELECT number * 3 AS n FROM numbers(1000)) AS r
ON l.number = r.n SETTINGS query_plan_convert_join_to_in = 1, log_comment = 'join to in';
SQL
)

# For each form: the sets filled, how many spilled, and whether the lookups read the disk. The refresh of
# `atomic.rmv` runs in the database of the view.
REPORT=$(cat <<'SQL'
SYSTEM FLUSH LOGS query_log;
SELECT 'report', log_comment, sum(ProfileEvents['SetsBuiltFromSubquery']), sum(ProfileEvents['SetsSpilledToDisk']),
    max(ProfileEvents['ExternalSetReadBlocks'] > 0)
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database IN (currentDatabase(), 'atomic')
GROUP BY log_comment
ORDER BY min(event_time_microseconds);
SQL
)

for threshold in 0 1; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${threshold}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 \
        --max_bytes_before_external_set "${threshold}" --multiquery <<< "${QUERIES}
${REPORT}" > "${LOCAL_DIR}/${threshold}.out"
done

# The results match in memory and on disk. With 1 byte every set spills before its first chunk, and without a
# threshold none does.
diff -u <(grep -v '^report' "${LOCAL_DIR}/0.out") <(grep -v '^report' "${LOCAL_DIR}/1.out")
grep -v '^report' "${LOCAL_DIR}/1.out"
grep '^report' "${LOCAL_DIR}/1.out"
grep '^report' "${LOCAL_DIR}/0.out" | awk -F'\t' '{ built += $3; spilled += $4 } END { print "without a threshold", built, spilled }'

# A lightweight `DELETE` and an `UPDATE` mutate three partitions. The tasks of the parts share the sets of a
# mutation through the prepared sets cache while they overlap, so the number of sets built depends on their
# timing, but every set spills with a threshold of 1 byte and none without it. The log does not attribute
# mutations to their queries, so the events of the process count the sets.
for threshold in 0 1; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/mutations-${threshold}" --max_bytes_before_external_set "${threshold}" \
        --multiquery > "${LOCAL_DIR}/mutations-${threshold}.out" <<'SQL'
CREATE TABLE d (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY intDiv(k, 10000) ORDER BY k;
INSERT INTO d SELECT number, 0 FROM numbers(30000);
DELETE FROM d WHERE k IN (SELECT number * 3 FROM numbers(10000));
ALTER TABLE d UPDATE v = 1 WHERE k IN (SELECT number * 5 FROM numbers(10000)) SETTINGS mutations_sync = 2;
SELECT 'mutations', count(), sum(v) FROM d;
WITH (SELECT sum(value) FROM system.events WHERE event = 'SetsBuiltFromSubquery') AS built,
    (SELECT sum(value) FROM system.events WHERE event = 'SetsSpilledToDisk') AS spilled
SELECT 'mutations', (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'd' AND active),
    built > 0, multiIf(spilled = 0, 'none spilled', spilled = built, 'all spilled', 'some spilled');
SQL
done
diff -u <(head -n 1 "${LOCAL_DIR}/mutations-0.out") <(head -n 1 "${LOCAL_DIR}/mutations-1.out")
cat "${LOCAL_DIR}/mutations-1.out"
tail -n 1 "${LOCAL_DIR}/mutations-0.out"

# The set of lazy `FINAL` serves only index analysis, which needs its values, so it stays in memory with a
# threshold of 1 byte and lazy `FINAL` applies. The parts are never merged.
for threshold in 0 1; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/lazy-final-${threshold}" --max_bytes_before_external_set "${threshold}" \
        --send_logs_level trace --multiquery > "${LOCAL_DIR}/lazy-final-${threshold}.out" \
        2> "${LOCAL_DIR}/lazy-final-${threshold}.log" <<'SQL'
CREATE TABLE r (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO r SELECT number, number FROM numbers(100000);
INSERT INTO r SELECT number, number + 1 FROM numbers(1000);
SELECT 'lazy final', count(), sum(v) FROM r FINAL WHERE v < 500 SETTINGS query_plan_optimize_lazy_final = 1;
SELECT 'lazy final', (SELECT sum(value) FROM system.events WHERE event = 'SetsSpilledToDisk');
SQL
done
diff -u "${LOCAL_DIR}/lazy-final-0.out" "${LOCAL_DIR}/lazy-final-1.out"
cat "${LOCAL_DIR}/lazy-final-1.out"
for threshold in 0 1; do
    echo "lazy final applied with threshold ${threshold}: $(grep -c 'Lazy FINAL enabled' "${LOCAL_DIR}/lazy-final-${threshold}.log" || true)"
done
