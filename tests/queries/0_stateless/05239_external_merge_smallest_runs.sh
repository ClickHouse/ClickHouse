#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-merge-smallest.XXXXXX")
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
]


def check(name, sql, expected, fan_in):
    log = root / (name + '.log')
    with log.open('w') as stderr:
        result = subprocess.run(command + [f'--max_external_merge_fan_in={fan_in}', '--query', sql],
                                stdout=subprocess.PIPE, stderr=stderr, text=True, timeout=120)
    text = log.read_text()
    assert result.returncode == 0, (name, text)
    assert result.stdout.strip() == expected, (name, result.stdout, expected)
    groups = [int(n) for n in re.findall(r'Starting intermediate external merge with (\d+) inputs', text)]
    assert groups and all(2 <= n <= fan_in for n in groups), (name, groups, text)
    assert not list((root / 'data' / 'tmp').glob('tmp*')), name
    print(name, 'ok', flush=True)
    return groups, text


# The first file is much larger than the others. Reducing 65 files to 64 must merge two small files.
groups, log = check('smallest_sort_runs', """
    SELECT count(), sum(length(payload)) FROM
    (SELECT cityHash64(number) AS k, arrayStringConcat(arrayMap(i -> hex(reinterpretAsFixedString(cityHash64(number, i))),
                    range(if(number < 4096, 64, 1)))) AS payload
     FROM numbers(266240) ORDER BY k, payload)
    """, '266240\t8388608', 64)
assert groups == [2], groups
start = log.index('Starting intermediate external merge')
end = log.index('Finished intermediate external merge')
pattern = r'Done writing part of data into temporary file .*?, compressed ([\d.]+) (\w+),'
units = {'B': 1, 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}
initial_sizes = [float(size) * units[unit] for size, unit in re.findall(pattern, log[:start])]
merged_sizes = [float(size) * units[unit] for size, unit in re.findall(pattern, log[start:end])]
assert len(initial_sizes) == 65 and len(merged_sizes) == 1, (initial_sizes, merged_sizes)
assert initial_sizes[0] > 10 * max(initial_sizes[1:]), initial_sizes
assert merged_sizes[0] < initial_sizes[0] / 4, (initial_sizes[0], merged_sizes)

# Repeated keys cross several merge levels for both comparison representations. The ordered cases
# retain a hidden sort column with a wide first block, so smaller files cannot replace the earliest rows.
for generic in (False, True):
    key = '[number % 8192]' if generic else 'number % 8192'
    key_value = 'k[1]' if generic else 'k'
    for ordered in (False, True):
        order = """ORDER BY tuple(number, arrayStringConcat(arrayMap(
            i -> hex(reinterpretAsFixedString(cityHash64(number, i))), range(if(number < 4096, 64, 1)))))""" if ordered else ''
        expected_order = f'groupArray({key_value}) = range(toUInt64(8192))' if ordered else '1'
        groups, log = check(f'distinct_{"fingerprint" if generic else "typed"}_{"ordered" if ordered else "unordered"}', f"""
            SELECT count(), uniqExact(k), sum({key_value}), {expected_order} FROM
            (SELECT DISTINCT {key} AS k FROM numbers(65536) {order})
            """, '8192\t8192\t33550336\t1', 2)
        assert len(groups) > 2, groups
        assert f'restore input order: {str(ordered).lower()}' in log, log
TEST
