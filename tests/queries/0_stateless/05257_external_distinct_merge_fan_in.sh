#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-merge-fan-in.XXXXXX")
export LOCAL_DIR
trap 'rm -rf "${LOCAL_DIR}"' EXIT

PYTHONPATH="$CUR_DIR/helpers" python3 - <<'PY'
from external_merge_fan_in import check


# Typed and fingerprint keys repeat across run boundaries.
check('distinct_typed', '''
    SELECT count(), uniqExact(k), sum(k) FROM
    (SELECT DISTINCT number % 10000 AS k FROM numbers(32768))
    ''', '10000\t10000\t49995000')
check('distinct_fingerprints', '''
    SELECT count(), uniqExact(k) FROM
    (SELECT DISTINCT [number % 1000, number % 7] AS k FROM numbers(32768))
    ''', '7000\t7000')
check('distinct_nullable_lc', """
    SELECT count(), uniqExact(tuple(a, b)) FROM
    (SELECT DISTINCT if(number % 5 = 0, NULL, number % 10000) AS a,
        toLowCardinality(toString(number % 10000)) AS b FROM numbers(32768))
    """, '10000\t10000')

# Final `DISTINCT` limits apply after intermediate runs have been reduced.
for mode in ('break', 'throw'):
    check(f'distinct_{mode}', """
        SELECT count() BETWEEN 5000 AND 9096 FROM
        (SELECT DISTINCT number % 10000 AS k FROM numbers(32768))
        """, '1', extra=('--max_rows_in_distinct=5000', f'--distinct_overflow_mode={mode}',
                          '--allow_preliminary_distinct_abandoning=1'),
        error='SET_SIZE_LIMIT_EXCEEDED' if mode == 'throw' else None)
PY
