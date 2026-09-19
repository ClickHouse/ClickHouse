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
    DROP DICTIONARY IF EXISTS d_dep;
    DROP TABLE IF EXISTS t_dep_src;
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

# The same ordering for the DatabaseCatalog dependency edges. A dictionary is used because the default
# check reads loading_dependencies, where a materialized view, a view, Merge and Distributed register
# nothing, so DROP TABLE of their source is not refused in the first place.
$CLICKHOUSE_CLIENT -m -q "
SET ignore_drop_queries_probability = 0;
SET ast_fuzzer_runs = 0;

CREATE TABLE t_dep_src (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY d_dep (id UInt64, v String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_dep_src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(0);

SELECT 'source protected while the dictionary is alive';
DROP TABLE t_dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }

SYSTEM ENABLE FAILPOINT drop_table_fail_before_metadata_drop;
DROP DICTIONARY d_dep; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT drop_table_fail_before_metadata_drop;

SELECT 'dictionaries after the failed drop', count() FROM system.tables WHERE database = currentDatabase() AND name = 'd_dep';
SELECT 'source still protected after the failed drop';
DROP TABLE t_dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }

DROP DICTIONARY d_dep;
DROP TABLE t_dep_src;
SELECT 'source droppable after the successful drop', count() FROM system.tables WHERE database = currentDatabase() AND name IN ('d_dep', 't_dep_src');
"
