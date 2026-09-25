#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-merge-reservation.XXXXXX")
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
units = {'B': 1, 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}
queries = {
    'sort': 'SELECT number AS k FROM numbers(12288) ORDER BY k + 1 DESC',
    'distinct': 'SELECT DISTINCT number AS k FROM numbers(12288)',
}

# Three initial runs require one two-input intermediate merge at a fan-in limit of two.
# Check the requested reservation rather than relying on the machine's current free disk space.
for kind, query in queries.items():
    for fan_in, free_space in ((2, 0), (2, 65536), (0, 65536)):
        name = f'{kind}_{fan_in}_{free_space}'
        log_path = root / f'{name}.log'
        with log_path.open('w') as stderr:
            result = subprocess.run(command + [
                f'--max_external_merge_fan_in={fan_in}',
                f'--min_free_disk_space_for_temporary_data={free_space}',
                '--query', f'SELECT count(), sum(k) FROM ({query})',
            ], stdout=subprocess.PIPE, stderr=stderr, text=True, timeout=60)
        log = log_path.read_text()
        assert result.returncode == 0, (name, log)
        assert result.stdout.strip() == f'12288\t{12288 * 12287 // 2}', (name, result.stdout)
        initial = re.search(r'Preparing external merge with (\d+) files', log)
        assert initial and int(initial[1]) == 3, (name, log)
        initial_end = initial.start()
        reservations = [float(size) * units[unit] for size, unit in re.findall(
            r'Reserved ([\d.]+) (\w+) on local disk', log[initial_end:])]
        if fan_in == 0:

            # The final merge streams its output and does not reserve another temporary file.
            assert not reservations, (name, reservations)
            assert 'Starting intermediate external merge' not in log, (name, log)
        else:
            sizes = re.findall(
                r'Done writing part of data into temporary file .*?, compressed ([\d.]+) (\w+), '
                r'uncompressed ([\d.]+) (\w+)', log[:initial_end])
            runs = sorted((float(c) * units[cu], float(u) * units[uu]) for c, cu, u, uu in sizes)
            assert len(runs) == 3 and len(reservations) == 1, (name, runs, reservations)
            assert log.count('Finished intermediate external merge of 2 inputs') == 1, (name, log)
            input_bytes = sum(uncompressed for _, uncompressed in runs[:2])

            # Reserve the selected inputs' uncompressed bytes plus the free-space floor. Trace sizes
            # are rounded, so allow one percent for formatting while excluding the third run's size.
            reserved_bytes = reservations[0] - free_space
            assert 0.99 * input_bytes <= reserved_bytes <= 1.01 * input_bytes, (name, runs, reservations)
        assert not list((root / 'data' / 'tmp').glob('tmp*')), name
        print(name, 'ok', flush=True)
TEST
