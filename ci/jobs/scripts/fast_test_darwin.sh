#!/bin/bash
# macOS Fast test wrapper.
#
# macOS does not auto-route 127.0.0.0/8, so the remote()/cluster() stateless
# tests need 127.0.0.2+ aliased on lo0 to be reachable. The macos_m2 runner is
# reused across jobs, so a leaked alias makes 127.0.0.2+ look local to a later
# job and breaks its distributed-EXPLAIN tests. Setup and teardown live here,
# not in praktika pre/post hooks, because praktika does not propagate hook exit
# codes to job status, so a hook cannot fail the job. Both are idempotent and
# fail-closed: setup skips an already-present alias and aborts on a failed add;
# teardown skips an absent alias and fails the job on a failed removal. The
# fast_test.py exit code is preserved.
#
# No `set -e`: the test exit code must be captured and the teardown must always
# run before exiting with it.

for i in $(seq 2 21); do
    ifconfig lo0 | grep -qF "127.0.0.$i " || sudo ifconfig lo0 alias 127.0.0.$i up || exit 1
done

# Forward praktika's appended run selectors (--test, --param, ...) to fast_test.py.
python3 ./ci/jobs/fast_test.py "$@"
rc=$?

for i in $(seq 2 21); do
    if ifconfig lo0 | grep -qF "127.0.0.$i "; then
        sudo ifconfig lo0 -alias 127.0.0.$i || rc=1
    fi
done

exit $rc
