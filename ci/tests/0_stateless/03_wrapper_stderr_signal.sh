#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
#
# Deliberately carries no no-{build} tag: the fixture runs no queries, so it is
# build-flavor independent and must be selectable against any binary (the CI
# Tests job uses a master release build, a developer's is typically debug).

# Fixture for ci/tests/test_test_process_does_not_hold_runner_stdio.py.
#
# The test needs a per-test process that writes to its own stderr and *then*
# dies from a signal: that is the only shape which makes the wrapper shell emit
# a job-control diagnostic (`/bin/bash: line 1: NNN Segmentation fault ...`) to
# the wrapper's own stderr while the test's own stderr is already in the same
# file.  It is what pins the append-mode open in `run_single_test`: a
# non-append descriptor there starts at offset 0 and overwrites the test's own
# stderr.
#
# The line carries a ` <Fatal> ` substring so the collected stderr is also the
# shape `process_result_impl` promotes to `SERVER_DIED`, which is exactly the
# consequence a clobbered file would silently disarm.
echo '2026.01.01 00:00:00.000000 [ 1 ] {} <Fatal> FIXTURE-OWN-STDERR-LINE: the test wrote this to its own stderr' >&2
kill -SEGV $$
