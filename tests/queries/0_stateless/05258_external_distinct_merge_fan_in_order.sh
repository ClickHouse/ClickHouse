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


# Sorting by a non-key column requires retaining the first payload and restoring arrival order.
check('distinct_payload', '''
    SELECT count(), min(c), groupArray(k) = range(toUInt64(10000)) FROM
    (SELECT DISTINCT if(number < 10000, number, toUInt64((10000 - number % 10000) % 10000)) AS k,
        'constant' AS c FROM numbers(32768) ORDER BY number + 1)
    ''', '10000\tconstant\t1')
check('distinct_order', '''
    SELECT count(), groupArray(k) = arrayReverse(range(toUInt64(10000))) FROM
    (SELECT DISTINCT number % 10000 AS k FROM numbers(32768) ORDER BY k + 1 DESC)
    ''', '10000\t1')

# A full key chunk exceeds the run's size floor, while the last chunk compacts to 16 keys.
# The tail stays in memory both at the file limit and when an intermediate merge is required.
for num_files, tail_merge in ((2, 'at_cap'), (3, 'after_intermediate')):
    unique_rows = num_files * 16384
    groups, log = check(f'distinct_tail_{tail_merge}', f'''
        SELECT count(), uniqExact(k) FROM
        (SELECT DISTINCT if(number < {unique_rows}, number, {unique_rows} + number % 16) AS k
         FROM numbers({unique_rows + 16384}))
        ''', f'{unique_rows + 16}\t{unique_rows + 16}',
        fan_in=2, intermediate=num_files > 2,
        extra=('--max_block_size=16384', '--max_bytes_before_external_distinct=65536'))
    assert f'temporary runs: {num_files}, in-memory chunks: 1,' in log, log
    assert groups == ([] if num_files == 2 else [2]), groups
    assert 'Starting final external merge with 2 files and 1 in-memory inputs' in log, log
PY
