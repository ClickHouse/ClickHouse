#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-merge-unlimited.XXXXXX")
export LOCAL_DIR
trap 'rm -rf "${LOCAL_DIR}"' EXIT

python3 - <<'TEST'
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
    '--max_untracked_memory=0', '--allow_preliminary_distinct_abandoning=0', '--optimize_distinct_in_order=0',
    '--query_plan_remove_redundant_sorting=0', '--logger.console', '--logger.level=trace',
    '--print-profile-events',
]

with (root / 'settings.log').open('w') as stderr:
    result = subprocess.run(command + ['--multiquery', '--query', """
        SELECT getSetting('max_external_merge_fan_in');
        SELECT getSetting('max_external_merge_fan_in') SETTINGS compatibility = '26.9';
        SELECT getSetting('max_external_merge_fan_in') SETTINGS compatibility = '26.10';
        SELECT getSetting('max_external_merge_fan_in')
            SETTINGS compatibility = '26.9', max_external_merge_fan_in = 2;
        """], stdout=subprocess.PIPE, stderr=stderr, text=True, timeout=30)
assert result.returncode == 0, (root / 'settings.log').read_text()
assert result.stdout.splitlines() == ['64', '0', '64', '2'], result.stdout
print('settings ok')

# More than 64 initial runs must enter a single final merge. Both an explicit zero and an older
# compatibility version disable intermediate merges, including when `DISTINCT` must restore input order.
queries = [
    ('sort', """
        SELECT count(), groupArray(k) = arraySort(groupArray(k))
        FROM (SELECT cityHash64(number) AS k FROM numbers(266240) ORDER BY k)
        """, '266240\t1'),
    ('distinct', """
        SELECT count(), uniqExact(k), sum(k)
        FROM (SELECT DISTINCT number % 8192 AS k FROM numbers(266240))
        """, '8192\t8192\t33550336'),
    ('distinct_ordered', """
        SELECT count(), groupArray(k) = range(toUInt64(8192))
        FROM (SELECT DISTINCT number % 8192 AS k FROM numbers(266240) ORDER BY number + 1)
        """, '8192\t1'),
]
for mode, setting in (('explicit', 'max_external_merge_fan_in = 0'),
                      ('compatibility', "compatibility = '26.9'")):
    for shape, query, expected in queries:
        name = f'{mode}_{shape}'
        log = root / (name + '.log')
        with log.open('w') as stderr:
            result = subprocess.run(command + ['--query', query + ' SETTINGS ' + setting],
                                    stdout=subprocess.PIPE, stderr=stderr, text=True, timeout=120)
        text = log.read_text()
        assert result.returncode == 0, (name, text)
        assert result.stdout.strip() == expected, (name, result.stdout, expected)

        # Order restoration has its own sorter and can produce fewer files than the `DISTINCT` merge.
        logger = 'ExternalDistinctTransform' if shape.startswith('distinct') else 'MergeSortingTransform'
        finals = [int(n) for n in re.findall(rf'{logger}: Starting final external merge with (\d+) files', text)]
        assert finals and all(n > 64 for n in finals), (name, finals, text)
        assert 'fan-in limit: 0' in text, (name, text)
        assert 'Starting intermediate external merge' not in text, (name, text)
        for event in ('ExternalProcessingIntermediateMerge', 'ExternalProcessingIntermediateMergeInputs'):
            assert sum(int(n) for n in re.findall(rf'{event}: (\d+) \(increment\)', text)) == 0, name
        assert not list((root / 'data' / 'tmp').glob('tmp*')), name
        print(name, 'ok', flush=True)
TEST
