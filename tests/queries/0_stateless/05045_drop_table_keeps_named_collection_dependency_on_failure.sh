#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# no-parallel: the failpoint below is server-global, so a concurrent DROP TABLE would consume it.
# no-replicated-database: there the DROP is enqueued in the replicated DDL log and returns before the
# local drop runs, so the injected failure never reaches the client.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NC_NAME="nc_drop_failure_${CLICKHOUSE_DATABASE}"

cleanup()
{
    # The table goes before the collection: the reverse order leaves a table whose collection is gone,
    # which is the unloadable state this test is about.
    $CLICKHOUSE_CLIENT -m -q "
    SYSTEM DISABLE FAILPOINT drop_table_fail_before_metadata_drop;
    SET ignore_drop_queries_probability = 0;
    SET ast_fuzzer_runs = 0;
    DROP TABLE IF EXISTS t_drop_failure;
    DROP NAMED COLLECTION IF EXISTS ${NC_NAME};
    " >/dev/null 2>&1
}
trap cleanup EXIT

# ignore_drop_queries_probability = 0: the stress runner sets it to 0.2, which turns a DROP into a no-op.
# ast_fuzzer_runs = 0: the stress profile enables the server-side AST fuzzer, which can rewrite a
# DROP TABLE into a DETACH and defeat the expected errors below.
$CLICKHOUSE_CLIENT -m -q "
SET ignore_drop_queries_probability = 0;
SET ast_fuzzer_runs = 0;

DROP NAMED COLLECTION IF EXISTS ${NC_NAME};
CREATE NAMED COLLECTION ${NC_NAME} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t_drop_failure (x UInt32) ENGINE = URL(${NC_NAME});

SYSTEM ENABLE FAILPOINT drop_table_fail_before_metadata_drop;
DROP TABLE t_drop_failure; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT drop_table_fail_before_metadata_drop;

SELECT 'tables after the failed drop', count() FROM system.tables WHERE database = currentDatabase() AND name = 't_drop_failure';
DROP NAMED COLLECTION ${NC_NAME}; -- { serverError NAMED_COLLECTION_IS_USED }

DROP TABLE t_drop_failure;
DROP NAMED COLLECTION ${NC_NAME};
SELECT 'tables after the successful drop', count() FROM system.tables WHERE database = currentDatabase() AND name = 't_drop_failure';
"
