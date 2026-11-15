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

# The host is reused across jobs, so print what earlier jobs left behind in memory,
# swap and disk. Read-only; it never changes the exit code.
print_host_state() {
    echo "=== macOS host state at job $1 ==="
    uptime
    sysctl hw.memsize kern.boottime vm.swapusage kern.memorystatus_level
    echo "runner-init: provisioned $(cat ~/.clickhouse-ci-runner-init-version 2>/dev/null), running $(grep -m1 -o 'version: int = [0-9]*' /tmp/runner-init.py 2>/dev/null)"
    ls -l /System/Volumes/VM
    timeout 30 diskutil apfs list | grep -E 'APFS Volume Disk|Mount Point|Capacity Consumed'
    echo "diskutil exit status ${PIPESTATUS[0]}"
    local deleted lsof_rc
    deleted=$(timeout 60 sudo -n lsof -nP +L1 2>/dev/null | awk '$5 == "REG"'; exit "${PIPESTATUS[0]}")
    lsof_rc=$?
    echo "$deleted" | awk -v rc="$lsof_rc" 'NF {d++} NF && !seen[$6 " " $9]++ {n++; s += $7} END {printf "open but deleted files: %d (%d descriptors), %.1f GiB (lsof exit status %d)\n", n, d, s / 2^30, rc}'
    echo "$deleted" | sort -k7,7 -rn | awk '!seen[$6 " " $9]++' | head -n 5 | cut -c1-200
    ps -axm -o pid,ppid,user,etime,rss,command | head -n 11 | cut -c1-200
    if [ "$1" = start ] && [ "$(df -P /System/Volumes/Data | awk 'NR == 2 {print $5 + 0}')" -ge 60 ]; then
        timeout 120 sudo -n du -xk -d 4 /System/Volumes/Data 2>/dev/null | awk '$1 >= 2^20' | sort -rn | head -n 30
        echo "du exit status ${PIPESTATUS[0]}"
    fi
}

print_host_state start

for i in $(seq 2 21); do
    ifconfig lo0 | grep -qF "127.0.0.$i " || sudo ifconfig lo0 alias 127.0.0.$i up || exit 1
done

# Match the Linux fast-test image timezone (its Dockerfile sets ENV TZ=Europe/Amsterdam) so
# that timezone-dependent test references reproduce; the macOS runner's default timezone differs.
export TZ=Europe/Amsterdam

# Forward praktika's appended run selectors (--test, --param, ...) to fast_test.py.
python3 ./ci/jobs/fast_test.py "$@"
rc=$?

for i in $(seq 2 21); do
    if ifconfig lo0 | grep -qF "127.0.0.$i "; then
        sudo ifconfig lo0 -alias 127.0.0.$i || rc=1
    fi
done

print_host_state end

exit $rc
