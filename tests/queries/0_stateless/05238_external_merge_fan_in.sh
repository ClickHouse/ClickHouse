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
import re
import subprocess

from external_merge_fan_in import check, command, root


# Full sorting and `LIMIT` both cross several merge levels. Constants must survive file readback.
for cap in (2, 3):
    check(f'sort_{cap}', '''
        SELECT count(), sum(number), min(c), groupArray(k) = arraySort(groupArray(k))
        FROM (SELECT number, cityHash64(number) AS k, 'constant' AS c
              FROM numbers(32768) ORDER BY k)
        ''', '32768\t536854528\tconstant\t1', fan_in=cap)
    check(f'sort_limit_{cap}', '''
        SELECT groupArray(number) = arrayReverse(range(toUInt64(32751), toUInt64(32768)))
        FROM (SELECT number FROM numbers(32768) ORDER BY number + 1 DESC LIMIT 17)
        ''', '1', fan_in=cap, extra=('--max_bytes_before_remerge_sort=0',))

# Exactly one more run than the cap requires only a two-input intermediate merge.
groups, _ = check('sort_boundary', '''
    SELECT count(), sum(number) FROM
    (SELECT number FROM numbers(266240) ORDER BY cityHash64(number))
    ''', '266240\t35441735680', fan_in=64)
assert groups == [2], groups
direct_sql = 'SELECT count() FROM (SELECT number FROM numbers(16384) ORDER BY cityHash64(number))'
_, log = check('sort_direct', direct_sql, '16384', fan_in=64, intermediate=False)

# The quota admits the initial runs but rejects an intermediate file before its inputs are released.
disk_usage = sum(int(n) for n in re.findall(r'ExternalProcessingCompressedBytesTotal: (\d+) \(increment\)', log))
assert disk_usage > 0, log
quota_config = root / 'disk_quota.xml'
quota_config.write_text(f'<clickhouse><max_temporary_data_on_disk_size>{disk_usage}</max_temporary_data_on_disk_size></clickhouse>')
check('intermediate_disk_limit', direct_sql, '',
      extra=(f'--config-file={quota_config}',),
      error='TOO_MANY_ROWS_OR_BYTES', final=False)

# `partial_merge` uses `join_on_disk_max_files_to_merge` independently of `max_external_merge_fan_in`.
groups, log = check('partial_merge_join', '''
    SELECT count(), sum(l.k), sum(r.v)
    FROM (SELECT number AS k FROM numbers(20000)) AS l
    INNER JOIN (SELECT number AS k, number + 1 AS v FROM numbers(20000)) AS r USING k
    SETTINGS join_algorithm = 'partial_merge', join_on_disk_max_files_to_merge = 2,
        max_rows_in_join = 4096, partial_merge_join_rows_in_right_blocks = 1024
    ''', '20000\t199990000\t200010000', fan_in=64)
assert set(groups) == {2}, groups
assert all(int(n) <= 2 for n in re.findall(r'Starting final external merge with (\d+) files', log))

# Invalid user values must be rejected before constructing a merge pipeline.
with (root / 'invalid_fan_in.log').open('w') as stderr:
    result = subprocess.run(command + ['--max_external_merge_fan_in=1', '--query',
        'SELECT number FROM numbers(100) ORDER BY cityHash64(number)'],
        stdout=subprocess.PIPE, stderr=stderr, text=True, timeout=30)
assert result.returncode != 0
assert 'BAD_ARGUMENTS' in (root / 'invalid_fan_in.log').read_text()
print('invalid_setting ok', flush=True)
PY
