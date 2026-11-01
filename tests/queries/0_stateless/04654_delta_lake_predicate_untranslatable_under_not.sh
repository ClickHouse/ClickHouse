#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/87390
# A sub-expression the delta-kernel predicate translator cannot handle was reported to the
# kernel as "the iterator is exhausted", which truncates the enclosing conjunction instead of
# dropping one child. In monotone position that is harmless, but under NOT it changes the
# answer: an empty conjunction normalizes to TRUE, so NOT(TRUE) skipped every data file, and a
# partially consumed conjunction produced a predicate narrower than the truth. Both returned
# too few rows with no error. The translator now emits an explicit `Unknown` predicate, which
# the kernel treats as "cannot decide" in any polarity.
#
# Every answer is asserted against the same query over the same bytes read as plain Parquet, so
# the expected values are derived rather than hand-copied. File counts are asserted too: losing
# pruning would be a silent performance regression that a correct-answers test cannot see.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_not"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"
mkdir -p "${ROOT}/t/_delta_log"

# Two data files with disjoint ranges, written by plain file() so no experimental Delta write
# path is involved. ClickHouse's own Delta writer emits stats containing only numRecords, and a
# statsless table can never prune, which would make every pruning assertion below vacuous. So
# the transaction log is hand-written with real per-column stats.
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${ROOT}/t/lo.parquet', Parquet, 'c0 Int8') SELECT toInt8(number) FROM numbers(50);
    INSERT INTO FUNCTION file('${ROOT}/t/hi.parquet', Parquet, 'c0 Int8') SELECT toInt8(100 + number) FROM numbers(27);
"

cat > "${ROOT}/t/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-t","format":{"provider":"parquet","options":{}},"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"c0\\",\\"type\\":\\"byte\\",\\"nullable\\":false,\\"metadata\\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
{"add":{"path":"lo.parquet","partitionValues":{},"size":685,"modificationTime":1700000000000,"dataChange":true,"stats":"{\\"numRecords\\":50,\\"minValues\\":{\\"c0\\":0},\\"maxValues\\":{\\"c0\\":49},\\"nullCount\\":{\\"c0\\":0}}"}}
{"add":{"path":"hi.parquet","partitionValues":{},"size":576,"modificationTime":1700000000000,"dataChange":true,"stats":"{\\"numRecords\\":27,\\"minValues\\":{\\"c0\\":100},\\"maxValues\\":{\\"c0\\":126},\\"nullCount\\":{\\"c0\\":0}}"}}
EOF

# Compare the Delta answer with the plain-Parquet answer over the identical bytes. Prints only
# the predicate and "ok"/the two counts, so the reference stays stable.
check() {
    local where="$1"
    local label="${2:-$1}"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT '${label//\'/\'\'}: ' || if(d = o, 'ok', 'MISMATCH delta=' || toString(d) || ' parquet=' || toString(o))
        FROM (
            SELECT
                (SELECT count() FROM deltaLakeLocal('${ROOT}/t') WHERE ${where}) AS d,
                (SELECT count() FROM file('${ROOT}/t/{lo,hi}.parquet', Parquet, 'c0 Int8') WHERE ${where}) AS o
        );
    "
}

# How many data files the Delta reader actually opened for this predicate, read from the
# DeltaLakeScannedFiles profile event in the same session right after the query. 1 means the
# pushed-down predicate skipped one of the two files; 2 means nothing was pruned. Answers alone
# cannot detect lost pruning, which is why this is asserted separately.
#
# The count is aggregated over the whole table rather than filtered to the event's own row: when a
# predicate skips every file the event is never incremented, so no row exists and a filtered query
# would print nothing at all instead of reporting a mismatch.
check_files() {
    local where="$1"
    local expected="$2"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT count() FROM deltaLakeLocal('${ROOT}/t') WHERE ${where} FORMAT Null;
        SELECT '${where//\'/\'\'}: ' || if(files = ${expected}, 'ok', 'files=' || toString(files) || ' expected ${expected}')
        FROM (SELECT sumIf(value, event = 'DeltaLakeScannedFiles') AS files FROM system.events);
    "
}

echo "-- wrong before the fix: the untranslatable child was dropped and the NOT inverted"
# c0 < c0 and c0 >= c0 are untranslatable because a comparison needs one side to be a literal;
# OR has no case in the translator at all. BETWEEN expands to a conjunction containing c0 >= c0,
# which is the shape reported in the issue.
check "NOT (c0 NOT BETWEEN c0 AND 100)"
check "NOT (c0 > 100 OR c0 < 0)"
check "NOT (c0 < c0)"
# A bare NOT (c0 >= c0) would be useless here: c0 >= c0 always holds, so the correct answer is 0
# and the wrong one is 0 too. Pairing it with a translatable conjunct makes it discriminating.
check "NOT (c0 >= c0 AND c0 < 100)"

echo "-- wrong before the fix: the conjunction was truncated, so a narrower predicate was pushed"
# Only the child after the untranslatable one is lost, so the answer depended on operand order:
# these two returned 50 and 0 respectively before the fix.
check "NOT (c0 >= 100 AND c0 < c0)"
check "NOT (c0 < c0 AND c0 >= 100)"
check "NOT (c0 >= 100 AND c0 < c0) AND c0 >= 0"
check "NOT (c0 < 100 AND c0 < c0)"

echo "-- wrong before the fix for any operand type, not just plain integers"
# The defect is in how the translator reports an untranslatable node, so it is independent of the
# operand type: Nullable, String and Array operands all reach the same path.
check "NOT (toNullable(c0) < toNullable(c0))"
check "NOT (toString(c0) < toString(c0) AND c0 >= 100)"
check "NOT ([c0] = [c0] AND c0 >= 100)"

echo "-- the Unknown predicate name must not be derived from the filter, which may hold non-UTF-8 bytes"
check "NOT (c0 < c0 AND toString(c0) > unhex('fffe'))" "NOT (c0 < c0 AND non-utf8 literal)"

echo "-- unchanged: predicates that already translated fully"
check "c0 >= 100"
check "NOT (c0 < 100)"
check "NOT (c0 > 100)"
check "NOT (c0 > 100 AND c0 > 100)"
check "NOT (c0 >= 100 AND c0 >= 100)"
check "NOT (NOT (c0 >= 100))"
check "c0 >= 100::Int16"
check "NOT (c0 >= 100::Int16)"
check "NOT (c0 >= 100::Int64)"

echo "-- unchanged: dropping an untranslatable child stays correct in monotone position"
check "c0 < c0"
check "c0 >= c0"
check "c0 > 200 OR c0 < 0"
check "c0 >= 100 AND c0 < c0"
check "c0 >= 100 AND (c0 > 1 OR c0 < 0)"
check "c0 >= 100 AND c0 != 105"
check "c0 < 50 AND NOT (c0 >= 100 AND c0 < c0)"
check "NOT (NOT (c0 < c0))"
check "c0 NOT BETWEEN c0 AND 100"

echo "-- file pruning must be preserved: these skip one of the two files"
check_files "c0 >= 100" 1
check_files "NOT (c0 < 100)" 1
check_files "NOT (c0 >= 100 AND c0 >= 100)" 1
check_files "NOT (NOT (c0 >= 100))" 1
check_files "c0 >= 100 AND c0 < c0" 1
check_files "c0 >= 100 AND (c0 > 1 OR c0 < 0)" 1
check_files "c0 >= 100 AND c0 != 105" 1
check_files "c0 >= 100::Int16" 1
check_files "NOT (c0 >= 100::Int16)" 1

echo "-- and these legitimately read both files"
check_files "NOT (c0 < c0)" 2
check_files "NOT (c0 > 100)" 2
check_files "c0 < c0" 2
check_files "NOT (c0 >= 100 AND c0 < c0)" 2

echo "-- a visitor exception is the same class: it must not skip files either"
# The failpoint makes the literal visitor throw. It is ONCE, so it self-disarms after one hit;
# a separate invocation keeps that hit on the intended query.
${CLICKHOUSE_LOCAL} --query "
    SYSTEM ENABLE FAILPOINT delta_kernel_fail_literal_visitor;
    SELECT 'failpoint NOT (c0 >= 100): ' || if(count() = 50, 'ok', 'got ' || toString(count())) FROM deltaLakeLocal('${ROOT}/t') WHERE NOT (c0 >= 100);
"

echo "-- but delta_lake_throw_on_engine_predicate_error still reports it"
${CLICKHOUSE_LOCAL} --query "
    SYSTEM ENABLE FAILPOINT delta_kernel_fail_literal_visitor;
    SELECT count() FROM deltaLakeLocal('${ROOT}/t') WHERE NOT (c0 >= 100) SETTINGS delta_lake_throw_on_engine_predicate_error = 1;
" 2>&1 | grep -c "FAULT_INJECTED"
