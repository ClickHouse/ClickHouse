#!/usr/bin/env bash
# Every SYSTEM ... FAILPOINT statement must reject a name that is not in the fail point registry.
#
# SYSTEM ENABLE FAILPOINT already did. DISABLE silently accepted anything. WAIT is the worst of the
# three: a mistyped name returned at once, so a test that meant to synchronise on a fail point just
# carried on into the race it was written to prevent. NOTIFY reported an error, but described it as a
# missing channel rather than a missing fail point.
#
# This is a clickhouse-local test on purpose: fail point state is process global, so asserting it
# against a shared server would race concurrent tests. Each clickhouse-local invocation owns its own
# state, which keeps the test parallel safe without a no-parallel tag.
#
# Carrier: dummy_failpoint. It is REGULAR, no other test references it, and it has no fiu_do_on site
# in src/, so enabling it has no side effect, it can never fire and disarm itself, and it never gets
# a wait channel. Waiting on it therefore returns immediately and cannot hang this test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Print the error code alone, so the reference stays stable across message wording changes.
code() { grep -oE 'Code: [0-9]+' <<<"$1" | head -1; }

# The NOTIFY rows also print which diagnostic fired: an unknown name and a registered-but-idle name
# both throw Code 36, so only the message tells the two error paths apart.
msg() { grep -oE 'Cannot find (channel for )?fail point' <<<"$1" | head -1; }

# Every statement runs under a timeout: if a rejection ever regresses into a wait, the test must fail
# loudly instead of hanging the job until the global CI timeout.
run() { timeout 30 ${CLICKHOUSE_LOCAL} --multiquery --query "$1" 2>&1; }

# The fix: a name that is not in the registry is rejected instead of silently accepted.
out=$(run "SYSTEM DISABLE FAILPOINT this_fail_point_does_not_exist")
echo "unknown_disable $(code "$out")"

# The symmetry the fix establishes. This already held before the fix.
out=$(run "SYSTEM ENABLE FAILPOINT this_fail_point_does_not_exist")
echo "unknown_enable $(code "$out")"

# Both waits used to return success on an unknown name, quietly dropping the synchronisation.
out=$(run "SYSTEM WAIT FAILPOINT this_fail_point_does_not_exist")
echo "unknown_wait $(code "$out")"

out=$(run "SYSTEM WAIT FAILPOINT this_fail_point_does_not_exist PAUSE")
echo "unknown_wait_pause $(code "$out")"

# NOTIFY threw Code 36 for an unknown name even before the fix, via the missing-channel path. Pin
# the message too, so this row only passes with the registration check, not the older error.
out=$(run "SYSTEM NOTIFY FAILPOINT this_fail_point_does_not_exist")
echo "unknown_notify $(code "$out") $(msg "$out")"

# Must not change: the check is on registration, never on enabled state. Disabling a registered fail
# point that is not currently enabled stays a silent no-op, because callers do idempotent cleanup (a
# trap on EXIT, a SCOPE_EXIT, a bulk reset) and must keep working.
out=$(run "SYSTEM DISABLE FAILPOINT dummy_failpoint")
rc=$?
echo "registered_not_enabled_disable rc=$rc out=[$out]"

# Must not change, same reason: a registered fail point that is not paused has no wait channel, and
# waiting on it stays an immediate no-op rather than an error.
out=$(run "SYSTEM WAIT FAILPOINT dummy_failpoint")
rc=$?
echo "registered_not_enabled_wait rc=$rc out=[$out]"

out=$(run "SYSTEM WAIT FAILPOINT dummy_failpoint PAUSE")
rc=$?
echo "registered_not_enabled_wait_pause rc=$rc out=[$out]"

# Must not change, and the counterpart of `unknown_notify`: NOTIFY on a registered fail point that
# nothing is waiting on keeps reporting the missing channel, not a missing fail point.
out=$(run "SYSTEM NOTIFY FAILPOINT dummy_failpoint")
echo "registered_notify_no_channel $(code "$out") $(msg "$out")"

# Must not change: enabling a registered fail point that has no pause semantics still leaves nothing
# to wait for, so the wait returns rather than blocking.
out=$(run "SYSTEM ENABLE FAILPOINT dummy_failpoint; SYSTEM WAIT FAILPOINT dummy_failpoint")
rc=$?
echo "registered_enabled_wait rc=$rc out=[$out]"

# Must not change: the working path still enables and disables.
timeout 30 ${CLICKHOUSE_LOCAL} --multiquery --query "
SYSTEM ENABLE FAILPOINT dummy_failpoint;
SELECT 'enabled', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
SYSTEM DISABLE FAILPOINT dummy_failpoint;
SELECT 'disabled', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
"

# 'still_armed' is a must-not-change control, not evidence: it reads 1 both before and after the fix,
# and pins that the rejected call leaves the fail point untouched rather than half-disabling it.
timeout 30 ${CLICKHOUSE_LOCAL} --multiquery --query "
SYSTEM ENABLE FAILPOINT dummy_failpoint;
SELECT 'armed', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
SYSTEM DISABLE FAILPOINT dummy_failpoin; -- { serverError BAD_ARGUMENTS }
SELECT 'still_armed', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
SYSTEM DISABLE FAILPOINT dummy_failpoint;
SELECT 'disarmed', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
"
