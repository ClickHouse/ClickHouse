#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel
# Tag rationale: transactions are unsupported in Ordinary databases; a Replicated database enqueues
# the ALTER as a distributed DDL, which is rejected inside a transaction, so no mutation entry is
# ever registered here and the failpoint can never fire; enables server-wide failpoints.
#
# `KILL MUTATION` must not destroy the `current_mutations_by_version` entry of a mutation whose
# transaction is already past its commit point: `MergeTreeTransaction::afterCommit` still has to
# stamp that mutation's CSN through `setMutationCSN`, which throws `LOGICAL_ERROR` from inside a
# `noexcept` function and aborts the server when the entry is gone.
#
# Arm 1 (witness) parks the commit past its commit point, immediately before the mutation-CSN loop:
# the kill must refuse with `cant_cancel` and leave the entry intact.
# Arm 2 (control) parks the commit before its CSN CAS, so the kill wins the claim and the `COMMIT`
# must fail with `INVALID_TRANSACTION`. Arm 2 also passes without the fix; it pins the atomicity of
# the claim, not the bug.
# Arm 3 interleaves the two: the killer is parked after resolving the transaction and before its
# claim, and the commit passes its commit point while the killer waits. Only claiming through the
# CAS refuses here; reading the CSN and then erasing is a TOCTOU that aborts the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

# Defense-in-depth: an early failure (e.g. a `SYSTEM WAIT FAILPOINT ... PAUSE` timeout) must not
# leave a server-wide failpoint active for later tests. Disabling a disabled failpoint is a no-op.
trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_commit_pause_before_mutation_csn" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_commit_pause_before_csn_cas" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT kill_mutation_pause_after_transaction_resolve" 2>/dev/null || true
' EXIT

echo '--- arm 1: commit past its commit point, kill must refuse'

# `v` is deliberately not part of the sorting key: `ALTER TABLE ... UPDATE` on a key column is
# rejected with CANNOT_UPDATE_COLUMN, so no mutation would ever be registered.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_kill_race;
    CREATE TABLE t_kill_race (n Int64, v Int64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO t_kill_race VALUES (1, 1);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_commit_pause_before_mutation_csn"

tx_async 1 "BEGIN TRANSACTION"
tx_async 1 "ALTER TABLE t_kill_race UPDATE v = v + 100 WHERE 1"
tx_async 1 "COMMIT"

# The commit is now past the ZK commit point, parked right before `setMutationCSN`.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_commit_pause_before_mutation_csn PAUSE"

MUTATION_ID=$($CLICKHOUSE_CLIENT -q "
    SELECT mutation_id FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_kill_race'
    ORDER BY mutation_id LIMIT 1
")

$CLICKHOUSE_CLIENT -q "
    KILL MUTATION WHERE database = currentDatabase() AND table = 't_kill_race'
        AND mutation_id = '$MUTATION_ID'
" | awk '{print "kill_status\t" $1}'

# The refusal must have left the entry registered, so the parked commit can still find it.
$CLICKHOUSE_CLIENT -q "
    SELECT 'mutation_entry_kept_during_pause', count()
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_kill_race' AND mutation_id = '$MUTATION_ID'
"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_commit_pause_before_mutation_csn"
tx_wait 1

# The commit must have succeeded: the row is updated and the mutation is still listed.
$CLICKHOUSE_CLIENT -q "SELECT 'data_after_commit', n, v FROM t_kill_race ORDER BY n"
$CLICKHOUSE_CLIENT -q "
    SELECT 'mutation_entry_kept_after_commit', count()
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_kill_race' AND mutation_id = '$MUTATION_ID'
"
$CLICKHOUSE_CLIENT -q "SELECT 'server_alive_after_arm1', 1"

echo '--- arm 2: commit not yet at its commit point, kill must win'

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_kill_race_2;
    CREATE TABLE t_kill_race_2 (n Int64, v Int64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO t_kill_race_2 VALUES (1, 1);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_commit_pause_before_csn_cas"

tx_async 2 "BEGIN TRANSACTION"
tx_async 2 "ALTER TABLE t_kill_race_2 UPDATE v = v + 100 WHERE 1"
# `tx_async` runs the query in a background job, so its output does not reach this script's stdout.
# Capture it in a file to assert on the rejection after `tx_wait`.
COMMIT_OUT="${CLICKHOUSE_TMP}/04812_commit_out.txt"
tx_async 2 "COMMIT" > "$COMMIT_OUT" 2>&1

# The commit has waited for the mutation and is parked before its `UnknownCSN -> CommittingCSN` CAS.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_commit_pause_before_csn_cas PAUSE"

MUTATION_ID_2=$($CLICKHOUSE_CLIENT -q "
    SELECT mutation_id FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_kill_race_2'
    ORDER BY mutation_id LIMIT 1
")

$CLICKHOUSE_CLIENT -q "
    KILL MUTATION WHERE database = currentDatabase() AND table = 't_kill_race_2'
        AND mutation_id = '$MUTATION_ID_2'
" | awk '{print "kill_status\t" $1}'

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_commit_pause_before_csn_cas"
tx_wait 2
# The COMMIT must have failed: its `UnknownCSN -> CommittingCSN` CAS lost to the kill's CAS.
grep -Eo 'INVALID_TRANSACTION' "$COMMIT_OUT" | uniq | awk '{print "commit_rejected\t" $1}'

# The kill won, so the data is unchanged and the mutation entry is gone.
$CLICKHOUSE_CLIENT -q "SELECT 'data_after_kill', n, v FROM t_kill_race_2 ORDER BY n"
$CLICKHOUSE_CLIENT -q "
    SELECT 'mutation_entry_removed', count()
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_kill_race_2' AND mutation_id = '$MUTATION_ID_2'
"
$CLICKHOUSE_CLIENT -q "SELECT 'server_alive_after_arm2', 1"

echo '--- arm 3: kill resolves the transaction, then the commit passes its commit point'

# Arms 1 and 2 each freeze one side for the whole window, so a plain CSN *read* behaves exactly like
# claiming through the CAS in both. Arm 3 is the discriminating interleaving: the killer resolves the
# transaction while the CSN is still `UnknownCSN`, parks, and only then does the commit pass its
# commit point. A read-then-erase implementation acts on that stale read and destroys an entry
# `setMutationCSN` still needs; claiming through the CAS refuses instead.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_kill_race_3;
    CREATE TABLE t_kill_race_3 (n Int64, v Int64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO t_kill_race_3 VALUES (1, 1);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_commit_pause_before_csn_cas"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT kill_mutation_pause_after_transaction_resolve"

tx_async 3 "BEGIN TRANSACTION"
tx_async 3 "ALTER TABLE t_kill_race_3 UPDATE v = v + 100 WHERE 1"
tx_async 3 "COMMIT"

# Step 1: the commit has waited for the mutation and is parked before its CSN CAS.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_commit_pause_before_csn_cas PAUSE"

MUTATION_ID_3=$($CLICKHOUSE_CLIENT -q "
    SELECT mutation_id FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_kill_race_3'
    ORDER BY mutation_id LIMIT 1
")

# Step 2: the killer resolves the transaction (seeing `UnknownCSN`) and parks before its claim.
KILL_OUT="${CLICKHOUSE_TMP}/04812_kill_out_3.txt"
$CLICKHOUSE_CLIENT -q "
    KILL MUTATION WHERE database = currentDatabase() AND table = 't_kill_race_3'
        AND mutation_id = '$MUTATION_ID_3'
" > "$KILL_OUT" 2>&1 &
KILL_PID=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT kill_mutation_pause_after_transaction_resolve PAUSE"

# Step 3: arm the next commit stop before releasing this one, so the commit cannot run past it.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_commit_pause_before_mutation_csn"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_commit_pause_before_csn_cas"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_commit_pause_before_mutation_csn PAUSE"

# Step 4: release the killer. Its resolve-time read of the CSN is now stale.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT kill_mutation_pause_after_transaction_resolve"
wait "$KILL_PID"
awk '{print "kill_status\t" $1}' "$KILL_OUT"
$CLICKHOUSE_CLIENT -q "
    SELECT 'mutation_entry_kept_after_stale_read', count()
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_kill_race_3' AND mutation_id = '$MUTATION_ID_3'
"

# Step 5: release the commit into `setMutationCSN`, which needs the entry the killer saw as killable.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_commit_pause_before_mutation_csn"
tx_wait 3

$CLICKHOUSE_CLIENT -q "SELECT 'data_after_commit_arm3', n, v FROM t_kill_race_3 ORDER BY n"
$CLICKHOUSE_CLIENT -q "SELECT 'server_alive_after_arm3', 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_kill_race; DROP TABLE t_kill_race_2; DROP TABLE t_kill_race_3"
