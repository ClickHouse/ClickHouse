#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-merge-tail.XXXXXX")
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
    '--path', str(root / 'data'), '--max_threads=1', '--max_block_size=16384',
    '--max_external_merge_fan_in=2', '--max_bytes_ratio_before_external_sort=0',
    '--max_bytes_ratio_before_external_distinct=0', '--max_untracked_memory=0',
    '--allow_preliminary_distinct_abandoning=0', '--optimize_distinct_in_order=0',
    '--query_plan_remove_redundant_sorting=0', '--logger.console', '--logger.level=trace',
    '--print-profile-events',
]

# A file limit of two permits two readers plus the in-memory tail. With three files, exactly one
# intermediate merge reduces the files to two, and the tail still joins only the final merge.
for num_files in (2, 3):
    unique_rows = num_files * 16384
    for shape in ('sort', 'distinct_fingerprint', 'distinct_ordered'):
        name = f'{shape}_{"at_cap" if num_files == 2 else "after_intermediate"}'
        if shape == 'sort':
            query = f"""
                SELECT count(), groupArray(k) = arraySort(groupArray(k))
                FROM (SELECT cityHash64(number) AS k FROM numbers({unique_rows + 16}) ORDER BY k)
                SETTINGS max_bytes_before_external_sort=65536
                """
        else:
            key = f'if(number < {unique_rows}, number, {unique_rows} + number % 16)'
            if shape == 'distinct_fingerprint':
                key = f'[{key}]'
                order = ''
                check = f'uniqExact(k) = {unique_rows + 16}'
            else:
                order = 'ORDER BY number + 1'
                check = f'groupArray(k) = range(toUInt64({unique_rows + 16}))'
            query = f"""
                SELECT count(), {check}
                FROM (SELECT DISTINCT {key} AS k FROM numbers({unique_rows + 16384}) {order})
                SETTINGS max_bytes_before_external_distinct=65536, max_bytes_before_external_sort=0
                """
        log = root / (name + '.log')
        with log.open('w') as stderr:
            result = subprocess.run(command + ['--query', query], stdout=subprocess.PIPE,
                                    stderr=stderr, text=True, timeout=120)
        text = log.read_text()
        assert result.returncode == 0, (name, text)
        assert result.stdout.strip() == f'{unique_rows + 16}\t1', (name, result.stdout)

        # Restoring `DISTINCT` input order performs its own external sort with the same file limit.
        num_stages = 2 if shape == 'distinct_ordered' else 1
        num_merges = num_stages * (num_files - 2)
        groups = [int(n) for n in re.findall(r'Starting intermediate external merge with (\d+) inputs', text)]
        assert groups == [2] * num_merges, (name, groups, text)
        finals = re.findall(r'Starting final external merge with (\d+) files and (\d+) in-memory inputs', text)
        assert finals == [('2', '1')] * num_stages, (name, finals, text)
        for event, expected in (('ExternalProcessingIntermediateMerge', num_merges),
                                ('ExternalProcessingIntermediateMergeInputs', 2 * num_merges)):
            value = sum(int(n) for n in re.findall(rf'{event}: (\d+) \(increment\)', text))
            assert value == expected, (name, event, value, expected)
        assert not list((root / 'data' / 'tmp').glob('tmp*')), name
        print(name, 'ok')
TEST
