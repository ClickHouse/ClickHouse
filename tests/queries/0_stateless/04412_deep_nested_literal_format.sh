#!/usr/bin/env bash
# Tags: no-fasttest, no-debug, no-sanitizers
# Deep literal construction is quadratic, so this regression requires an optimized build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `formatQuery` reaches literal formatting without type inference; native stack use depends on generated code.
python3 - "$CLICKHOUSE_CLIENT" --max_parser_depth=100000000 --max_query_size=1000000000 --format=TabSeparatedRaw <<'PY'
import re
import shlex
import subprocess
import sys

inner = 'SELECT ' + '[' * 20000 + '1' + ']' * 20000
query = 'SELECT formatQuery($$' + inner + '$$)\n'
result = subprocess.run(shlex.split(sys.argv[1]) + sys.argv[2:], input=query.encode(), capture_output=True)
formatted = result.returncode == 0 and result.stdout == (inner + '\n').encode() and not result.stderr
codes = re.findall(rb'\bCode: (\d+)\.', result.stderr)
guarded = (
    result.returncode == 306 % 256
    and not result.stdout
    and codes and set(codes) == {b'306'}
    and b'(TOO_DEEP_RECURSION)' in result.stderr
)
if not (formatted or guarded):
    sys.exit(
        f'Unexpected deep literal formatting result: exit={result.returncode}, '
        f'stdout({len(result.stdout)} bytes)={result.stdout[:200]!r}, '
        f'stderr({len(result.stderr)} bytes)={result.stderr[:2000]!r}'
    )
print('Deep literal formatting OK')
PY
