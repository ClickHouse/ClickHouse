#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-merge-groups.XXXXXX")
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
    '--path', str(root / 'data'), '--logger.console', '--logger.level=trace', '--print-profile-events',
]

# Poorly compressible keys fill the hash set before spilling, producing large suppression files.
# Subsequent keys compress well, so the ordinary group can remove file readers with much less disk I/O.
query = """
    SELECT count()
    FROM
    (
        SELECT DISTINCT if(number < 131072,
            arrayStringConcat(arrayMap(i -> reinterpretAsFixedString(cityHash64(number, i)), range(64))),
            concat(repeat('x', 512), toString(number))) AS k
        FROM numbers(1000000)
    )
    SETTINGS max_threads = 1, max_block_size = 4096, max_bytes_before_external_distinct = 134217728,
        max_bytes_ratio_before_external_distinct = 0, allow_preliminary_distinct_abandoning = 1,
        optimize_distinct_in_order = 0, max_external_merge_fan_in = 4,
        max_memory_usage = 2147483648, max_untracked_memory = 0
"""
with (root / 'query.log').open('w') as stderr:
    result = subprocess.run(command + ['--query', query], stdout=subprocess.PIPE, stderr=stderr,
                            text=True, timeout=120)
log = (root / 'query.log').read_text()
assert result.returncode == 0, log
assert result.stdout.strip() == '1000000', result.stdout

initial_end = log.index('Preparing external merge with')
initial_sizes = {'suppression': [], 'ordinary': []}
units = {'B': 1, 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}
for line in log[:initial_end].splitlines():
    run = re.search(r'Will dump DISTINCT (suppression|ordinary) run', line)
    if run:
        group = run.group(1)
    size = re.search(r'Done writing part of data into temporary file .*?, compressed ([\d.]+) (\w+),', line)
    if size:
        initial_sizes[group].append(float(size.group(1)) * units[size.group(2)])
assert all(len(sizes) >= 2 for sizes in initial_sizes.values()), initial_sizes

# The two groups need different numbers of merge inputs. Compare bytes per removed file rather than
# just the total bytes in each candidate merge, and require the cheaper group to be selected first.
num_files = sum(len(sizes) for sizes in initial_sizes.values())
costs = {}
for group, sizes in initial_sizes.items():
    inputs = min(4, num_files - 4 + 1, len(sizes))
    costs[group] = sum(sorted(sizes)[:inputs]) / (inputs - 1)
assert costs['ordinary'] < costs['suppression'] / 2, (initial_sizes, costs)
merges = [(int(inputs), int(group)) for inputs, group in re.findall(
    r'Starting intermediate external merge with (\d+) inputs \(group: (\d+) of 2,', log)]
assert merges[0][1] == 2, (merges, costs)
assert any(group == 1 for _, group in merges[1:]), merges

# Initial writes plus intermediate writes stay below twice the initial compressed size. Rewriting
# the large suppression group repeatedly exceeds this budget. Rounded trace sizes leave ample margin.
written_bytes = sum(int(value) for value in re.findall(
    r'ExternalProcessingCompressedBytesTotal: (\d+) \(increment\)', log))
initial_bytes = sum(sum(sizes) for sizes in initial_sizes.values())
assert initial_bytes < written_bytes < 2 * initial_bytes, (initial_sizes, written_bytes)
completed_inputs = sum(int(value) for value in re.findall(
    r'ExternalProcessingIntermediateMergeInputs: (\d+) \(increment\)', log))
assert completed_inputs == sum(inputs for inputs, _ in merges), (completed_inputs, merges)
assert 'Starting final external merge with 4 files' in log, log
assert not list((root / 'data' / 'tmp').glob('tmp*'))
print('cheaper_group_first ok')
print('group_reselection ok')
print('intermediate_write_budget ok')
PY
