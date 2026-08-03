#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

# On macOS, numpy uses Apple's Accelerate framework for BLAS, which multithreads via
# libdispatch (GCD). libdispatch is not fork-safe: forking (as the test runner does, heavily,
# to launch many parallel test subprocesses) while a GCD worker thread is active can leave a
# child process with a permanently locked internal mutex, hanging before any user code runs.
# Force single-threaded Accelerate to avoid spinning up that thread pool at all.
export VECLIB_MAXIMUM_THREADS=1

python3 "$CURDIR"/02294_anova_cmp.python
