#!/usr/bin/env bash
# Tags: no-darwin
# no-darwin: this test's import chain (numpy/pandas/scipy.stats) touches ~2400 files, and the
# macOS CI runners' EBS-backed root volume has a large per-file latency penalty on a cold read of
# many small files (measured ~27x slower than warm, confirmed generic to any many-small-file
# access pattern, not specific to Python/scipy/code-signing). Under the fast-test job's 9-way
# concurrent load, that cold-read penalty compounds enough to exceed the 60s test timeout. Verified
# directly on an arm_darwin CI instance: 100% failure rate historically whenever actually run
# (never skipped), reproducible on demand via `sudo purge` + reimport, and reproduced identically
# with synthetic files containing no code at all, ruling out anything ClickHouse/test-specific.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

python3 "$CURDIR"/02294_anova_cmp.python
