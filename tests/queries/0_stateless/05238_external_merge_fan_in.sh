#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-merge-fan-in.XXXXXX")
export LOCAL_DIR
trap 'rm -rf "${LOCAL_DIR}"' EXIT

python3 - <<'PY'
import os
import re
import shlex
import subprocess
from pathlib import Path

root = Path(os.environ['LOCAL_DIR'])
command = shlex.split(os.environ['CLICKHOUSE_LOCAL']) + [
    '--path', str(root / 'data'), '--max_threads=1', '--max_block_size=4096',
    '--max_bytes_before_external_sort=1', '--max_bytes_ratio_before_external_sort=0',
    '--max_bytes_before_external_distinct=1', '--max_bytes_ratio_before_external_distinct=0',
    '--max_untracked_memory=0', '--optimize_distinct_in_order=0',
    '--allow_preliminary_distinct_abandoning=0', '--query_plan_remove_redundant_sorting=0',
    '--logger.console', '--logger.level=trace', '--print-profile-events',
]


def check(name, sql, expected, fan_in=2, intermediate=True, extra=(), error=None, final=True):
    log = root / (name + '.log')
    with log.open('w') as stderr:
        overridden = {option.split('=', 1)[0] for option in extra}
        options = [option for option in command if option.split('=', 1)[0] not in overridden]
        result = subprocess.run(options + [f'--max_external_merge_fan_in={fan_in}', '--query', sql] + list(extra),
                                stdout=subprocess.PIPE, stderr=stderr, text=True, timeout=180)
    text = log.read_text()
    if error is None:
        assert result.returncode == 0, (name, text)
        assert result.stdout.strip() == expected, (name, result.stdout, expected)
    else:
        assert result.returncode != 0 and error in text, (name, text)
    groups = [int(n) for n in re.findall(r'Starting intermediate external merge with (\d+) inputs', text)]
    finals = [int(n) for n in re.findall(r'Starting final external merge with (\d+) files', text)]
    assert bool(groups) == intermediate, (name, groups, text[-10000:])
    assert bool(finals) == final and all(n <= fan_in for n in finals), (name, finals)
    assert all(2 <= n <= fan_in for n in groups), (name, groups)
    if error is None:

        # Completed merges count file inputs, while the direct path leaves both counters at zero.
        completed = [int(n) for n in re.findall(r'Finished intermediate external merge of (\d+) inputs', text)]
        for event, expected_value in (
            ('ExternalProcessingIntermediateMerge', len(completed)),
            ('ExternalProcessingIntermediateMergeInputs', sum(completed)),
        ):
            value = sum(int(n) for n in re.findall(rf'{event}: (\d+) \(increment\)', text))
            assert value == expected_value, (name, event, value, expected_value)
        assert len(completed) == len(groups), (name, completed, groups)
    assert not list((root / 'data' / 'tmp').glob('tmp*')), name
    print(name, 'ok')
    return groups, text


# Full sorting and `LIMIT` both cross several merge levels. Constants must survive file readback.
for cap in (2, 3):
    check(f'sort_{cap}', '''
        SELECT count(), sum(number), min(c), groupArray(k) = arraySort(groupArray(k))
        FROM (SELECT number, cityHash64(number) AS k, 'constant' AS c
              FROM numbers(262144) ORDER BY k)
        ''', '262144\t34359607296\tconstant\t1', fan_in=cap)
    check(f'sort_limit_{cap}', '''
        SELECT groupArray(number) = arrayReverse(range(toUInt64(262127), toUInt64(262144)))
        FROM (SELECT number FROM numbers(262144) ORDER BY number + 1 DESC LIMIT 17)
        ''', '1', fan_in=cap, extra=('--max_bytes_before_remerge_sort=0',))

# Typed and fingerprint keys repeat across run boundaries.
check('distinct_typed', '''
    SELECT count(), uniqExact(k), sum(k) FROM
    (SELECT DISTINCT number % 10000 AS k FROM numbers(262144))
    ''', '10000\t10000\t49995000')
check('distinct_fingerprints', '''
    SELECT count(), uniqExact(k) FROM
    (SELECT DISTINCT [number % 10000, number % 7] AS k FROM numbers(262144))
    ''', '70000\t70000')
check('distinct_nullable_lc', """
    SELECT count(), uniqExact(tuple(a, b)) FROM
    (SELECT DISTINCT if(number % 5 = 0, NULL, number % 10000) AS a,
        toLowCardinality(toString(number % 10000)) AS b FROM numbers(262144))
    """, '10000\t10000')

# Final `DISTINCT` limits apply after intermediate runs have been reduced.
for mode in ('break', 'throw'):
    check(f'distinct_{mode}', """
        SELECT count() BETWEEN 5000 AND 9096 FROM
        (SELECT DISTINCT number % 10000 AS k FROM numbers(262144))
        """, '1', extra=('--max_rows_in_distinct=5000', f'--distinct_overflow_mode={mode}',
                          '--allow_preliminary_distinct_abandoning=1'),
        error='SET_SIZE_LIMIT_EXCEEDED' if mode == 'throw' else None)

# Sorting by a non-key column requires retaining the first payload and restoring arrival order.
check('distinct_payload', '''
    SELECT count(), min(c), groupArray(k) = range(toUInt64(10000)) FROM
    (SELECT DISTINCT if(number < 10000, number, toUInt64((10000 - number % 10000) % 10000)) AS k,
        'constant' AS c FROM numbers(262144) ORDER BY number + 1)
    ''', '10000\tconstant\t1')
check('distinct_order', '''
    SELECT count(), groupArray(k) = arrayReverse(range(toUInt64(10000))) FROM
    (SELECT DISTINCT number % 10000 AS k FROM numbers(262144) ORDER BY k + 1 DESC)
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

# A larger threshold allows hashing to emit keys before spilling. Suppression files retain these keys
# through intermediate merges so the final merge does not emit them again.
_, log = check('distinct_suppression', '''
    SELECT count(), uniqExact(k) FROM
    (SELECT DISTINCT concat(repeat('x', 512), toString(number % 500000)) AS k FROM numbers(1000000))
    ''', '500000\t500000', extra=('--max_bytes_before_external_distinct=268435456',))
assert re.search(r'Extracting [1-9]\d* DISTINCT suppression keys', log)

# Exactly one more run than the cap requires only a two-input intermediate merge.
groups, _ = check('sort_boundary', '''
    SELECT count(), sum(number) FROM
    (SELECT number FROM numbers(4259840) ORDER BY cityHash64(number))
    ''', '4259840\t9073116282880', fan_in=64, extra=('--max_block_size=65536',))
assert groups == [2], groups
direct_sql = 'SELECT count() FROM (SELECT number FROM numbers(131072) ORDER BY cityHash64(number))'
_, log = check('sort_direct', direct_sql, '131072', fan_in=64, intermediate=False)

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
print('invalid_setting ok')
PY
