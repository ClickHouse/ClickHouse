#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
#
# Deliberately carries no no-{build} tag: the fixture runs no queries, so it is
# build-flavor independent and must be selectable against any binary (the CI
# Tests job uses a master release build, a developer's is typically debug).

# Fixture for ci/tests/test_test_process_does_not_hold_runner_stdio.py.
#
# The test needs a per-test wrapper process that is alive long enough to be
# inspected and to outlive the runner, so that we can check whether it holds
# the runner's stdio.  120s is far above the test's own hard bounds; the test
# always kills the process group in its teardown.
#
# The line below is deliberately written to plain stdout, so that it flows
# through the harness's own `> {stdout}` redirect into the runner-managed
# per-test stdout file, exactly like any real test's output does.  It is
# printed before the sleep, so that file is already populated while the
# wrapper is still alive.
echo 1
sleep 120
