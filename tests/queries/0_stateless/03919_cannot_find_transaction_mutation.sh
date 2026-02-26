#!/usr/bin/env bash
# Tags: no-parallel, no-ordinary-database, no-async-insert

# Test to reproduce: "Cannot find transaction X that started mutation Y for part Z"
# Scenario: Mutation created in transaction, transaction cleaned from log,
# mutation tries to execute on zombie parts

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

# Create table
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE test_txn_error (id UInt64, value String)
ENGINE = MergeTree() ORDER BY id
"

# "Step 1: Insert and merge"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_txn_error VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_txn_error VALUES (2, 'b')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_txn_error VALUES (3, 'c')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test_txn_error FINAL" 2>&1 | grep -v "ABORTED" || true

# "Step 2: TRUNCATE in transaction"
${CLICKHOUSE_CLIENT} --multiquery --query "
BEGIN TRANSACTION;
TRUNCATE TABLE test_txn_error;
COMMIT;
"

# "Step 3: Create mutation IN TRANSACTION (before zombie parts appear)"
${CLICKHOUSE_CLIENT} --multiquery --query "
BEGIN TRANSACTION;
ALTER TABLE test_txn_error UPDATE value = 'mutated' WHERE 1 SETTINGS mutations_sync=0;
COMMIT;
"

# "Step 4: Force TransactionLog cleanup"
# One commit is enough to trigger runUpdatingThread via ZK watch, which calls removeOldEntries().
# Poll until tail_ptr advances past the mutation's start_csn, confirming eviction from tid_to_csn.
DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database=currentDatabase() AND table='test_txn_error'")
MUTATION_CSN=$(grep -oP '(?<=tid: \()\d+' "${DATA_PATH}"mutation_4.txt 2>/dev/null | head -1)
${CLICKHOUSE_CLIENT} --multiquery --query "
BEGIN TRANSACTION; SELECT 1 FORMAT Null; COMMIT;
"
transaction_cleanup_ok=0
for _ in {1..30}; do
    result=$(${CLICKHOUSE_CLIENT} --query "
        SELECT transactionOldestSnapshot() > ${MUTATION_CSN}
    ")
    if [ "${result}" = "1" ]; then
        transaction_cleanup_ok=1
        break
    fi
    sleep 0.3
done
if [ "${transaction_cleanup_ok}" = "1" ]; then
    echo "Transaction cleanup: OK"
else
    echo "Transaction cleanup: FAILED"
fi

# "Step 5: DETACH to clear in-memory state"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE test_txn_error"

# "Step 6: ATTACH - zombie parts resurrect"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE test_txn_error"

# Check active parts count
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.parts
WHERE database=currentDatabase() AND table='test_txn_error' AND active
"

# "Step 7: Wait for mutation to finish"
wait_for_mutation test_txn_error mutation_4.txt

${CLICKHOUSE_CLIENT} --query "
SELECT mutation_id, is_done, latest_fail_reason
FROM system.mutations
WHERE database=currentDatabase() AND table='test_txn_error'
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_txn_error SYNC"
