#!/usr/bin/env bash
# Tags: no-fasttest

# `icebergTruncate` computes a different value depending on its width argument's TYPE, and the
# Iceberg read paths derive that type from a literal. A writer that builds the width at a wider
# unsigned type therefore stores a partition value the reader never computes, and a predicate that
# matches a stored row is pruned away: for `truncate[10]` over `-4..-1` the writer stored -6 where
# the Iceberg spec (and the reader) say -10.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=
TABLE_PATH=

cleanup()
{
    if [ -n "${TABLE}" ]; then
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
        rm -rf "${TABLE_PATH}"
    fi
}
trap cleanup EXIT

# $1 = table name suffix, $2 = source column type, $3 = PARTITION BY expression,
# $4 = stored-partition check: value | agree, $5 = transform width (only for `agree`)
run_case() {
    TABLE="t_${CLICKHOUSE_DATABASE}_$1"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
    rm -rf "${TABLE_PATH}"

    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${TABLE} (k $2, v Int32)
        ENGINE = IcebergLocal('${TABLE_PATH}')
        PARTITION BY $3
    "
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        INSERT INTO ${TABLE} SELECT number - 5, number FROM numbers(1, 8)
    "

    echo "=== $2 / $3 ==="

    ${CLICKHOUSE_CLIENT} --query "
        SELECT 'total', count() FROM ${TABLE};
        SELECT 'pred_negative', count() FROM ${TABLE} WHERE k = -1;
        SELECT 'pred_positive', count() FROM ${TABLE} WHERE k = 2;
        SELECT 'all_rows', groupArray(k) FROM (SELECT k FROM ${TABLE} ORDER BY k);
    "

    # The persisted partition value is the half a predicate cannot show, and its declared type is
    # what external Iceberg readers bind to.
    for manifest in $(find "${TABLE_PATH}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f); do
        case "$4" in
            value)
                ${CLICKHOUSE_CLIENT} --query "
                    SELECT 'stored',
                           toTypeName(tupleElement(tupleElement(data_file, 'partition'), 1)),
                           tupleElement(data_file, 'partition')
                    FROM file('${manifest}', Avro)
                "
                ;;
            # `truncate[100000]` of a negative value is not yet the value the Iceberg spec names, so
            # pinning it would bless a wrong constant. Assert the writer/reader AGREEMENT instead:
            # every row of a data file shares its partition value, so truncating the file's own
            # recorded minimum (field id 1 = `k`) reproduces it. Independent of partition pruning.
            agree)
                ${CLICKHOUSE_CLIENT} --query "
                    SELECT 'agrees',
                           toTypeName(tupleElement(tupleElement(data_file, 'partition'), 1)),
                           tupleElement(tupleElement(data_file, 'partition'), 1)
                             = icebergTruncate($5, reinterpretAsInt32(arrayFilter(x -> x.1 = 1, tupleElement(data_file, 'lower_bounds'))[1].2))
                    FROM file('${manifest}', Avro)
                "
                ;;
            *) echo "run_case: unknown stored-partition mode '$4'" >&2; exit 1 ;;
        esac
    # LC_ALL=C: a negative partition value carries a `-`, which glibc's non-C collations ignore at
    # the primary level, so `(-10)` and `(0)` would order differently per locale.
    done | LC_ALL=C sort

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
    rm -rf "${TABLE_PATH}"
    TABLE=
}

# A position-delete file is paired with its data file by exact partition value, so the mutation path
# and the insert path must agree on that value. This arm is a NON-REGRESSION CONTROL, not a fix
# witness: it is green on both binaries (before the fix both sides say -6, after it both say -10).
# It exists so that a later change moving one of the two seams and not the other reddens here
# instead of silently dropping deletes.
run_mutation_case() {
    TABLE="t_${CLICKHOUSE_DATABASE}_mut"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
    rm -rf "${TABLE_PATH}"

    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${TABLE} (k Int32, v Int32)
        ENGINE = IcebergLocal('${TABLE_PATH}')
        PARTITION BY icebergTruncate(10, k)
    "
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        INSERT INTO ${TABLE} SELECT number - 5, number FROM numbers(1, 8)
    "

    echo "=== mutation control: DELETE on Int32 / icebergTruncate(10, k) ==="

    # `v = 3` is the row k = -2, a negative source. The predicate deliberately avoids `k`: a
    # predicate on the partition column is answered by pruning, which is the read half this PR
    # already fixes, and would mask whether the delete itself was applied.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "
        ALTER TABLE ${TABLE} DELETE WHERE v = 3
    "
    ${CLICKHOUSE_CLIENT} --query "
        SELECT 'after_delete_total', count() FROM ${TABLE};
        SELECT 'after_delete_rows', groupArray(k) FROM (SELECT k FROM ${TABLE} ORDER BY k);
    "

    # The invariant is an EQUALITY between the two writers, so assert it as a comparison rather than
    # as two pinned constants: every position-delete entry must carry the partition value of a data
    # file. Degrades fail-closed -- no delete file at all gives 0 instead of 1.
    local entries=
    for manifest in $(find "${TABLE_PATH}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f); do
        entries="${entries}${entries:+ UNION ALL }SELECT tupleElement(data_file, 'content') AS content, toString(tupleElement(data_file, 'partition')) AS part FROM file('${manifest}', Avro)"
    done
    ${CLICKHOUSE_CLIENT} --query "
        WITH entries AS (${entries})
        SELECT 'delete_files', countIf(content = 1),
               'partition_matches_a_data_file', countIf(content = 1 AND part IN (SELECT part FROM entries WHERE content = 0)),
               'orphaned', countIf(content = 1 AND part NOT IN (SELECT part FROM entries WHERE content = 0))
        FROM entries
    "

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
    rm -rf "${TABLE_PATH}"
    TABLE=
}

# One arm per width-literal type `FieldToDataType` produces, because that type is what the defect
# turns on: UInt8 (W <= 255), UInt16, then UInt32.
run_case trunc_10 Int32 'icebergTruncate(10, k)' value
run_case trunc_1000 Int32 'icebergTruncate(1000, k)' value
run_case trunc_100000 Int32 'icebergTruncate(100000, k)' agree 100000
# The second source type Iceberg accepts for this transform carries the same defect.
run_case trunc_10_int64 Int64 'icebergTruncate(10, k)' value
# Control: the other transform taking a width shares the same seam and must not move.
run_case bucket_10 Int32 'icebergBucket(10, k)' value
run_mutation_case
