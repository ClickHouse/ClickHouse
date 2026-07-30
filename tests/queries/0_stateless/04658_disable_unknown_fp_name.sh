#!/usr/bin/env bash
# SYSTEM DISABLE FAILPOINT must reject a name that is not in the fail point registry.
#
# This is a clickhouse-local test on purpose: fail point state is process global, so asserting it
# against a shared server would race concurrent tests. Each clickhouse-local invocation owns its own
# state, which keeps the test parallel safe without a no-parallel tag.
#
# Carrier: dummy_failpoint. It is REGULAR, no other test references it, and it has no fiu_do_on site
# in src/, so enabling it has no side effect and it can never fire and disarm itself.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Print the error code alone, so the reference stays stable across message wording changes.
code() { grep -oE 'Code: [0-9]+' <<<"$1" | head -1; }

# The fix: a name that is not in the registry is rejected instead of silently accepted.
out=$(${CLICKHOUSE_LOCAL} --query "SYSTEM DISABLE FAILPOINT this_fail_point_does_not_exist" 2>&1)
echo "unknown_disable $(code "$out")"

# The symmetry the fix establishes. This already held before the fix.
out=$(${CLICKHOUSE_LOCAL} --query "SYSTEM ENABLE FAILPOINT this_fail_point_does_not_exist" 2>&1)
echo "unknown_enable $(code "$out")"

# Must not change: the check is on registration, never on enabled state. Disabling a registered fail
# point that is not currently enabled stays a silent no-op, because callers do idempotent cleanup (a
# trap on EXIT, a SCOPE_EXIT, a bulk reset) and must keep working.
out=$(${CLICKHOUSE_LOCAL} --query "SYSTEM DISABLE FAILPOINT dummy_failpoint" 2>&1)
rc=$?
echo "registered_not_enabled_disable rc=$rc out=[$out]"

# Must not change: the working path still enables and disables.
${CLICKHOUSE_LOCAL} --multiquery --query "
SYSTEM ENABLE FAILPOINT dummy_failpoint;
SELECT 'enabled', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
SYSTEM DISABLE FAILPOINT dummy_failpoint;
SELECT 'disabled', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
"

# 'still_armed' is a must-not-change control, not evidence: it reads 1 both before and after the fix,
# and pins that the rejected call leaves the fail point untouched rather than half-disabling it.
${CLICKHOUSE_LOCAL} --multiquery --query "
SYSTEM ENABLE FAILPOINT dummy_failpoint;
SELECT 'armed', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
SYSTEM DISABLE FAILPOINT dummy_failpoin; -- { serverError BAD_ARGUMENTS }
SELECT 'still_armed', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
SYSTEM DISABLE FAILPOINT dummy_failpoint;
SELECT 'disarmed', enabled FROM system.fail_points WHERE name = 'dummy_failpoint';
"
