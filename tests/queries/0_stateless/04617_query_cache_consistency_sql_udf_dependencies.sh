#!/usr/bin/env bash

# Tests that query_cache_use_only_when_data_was_not_changed fails closed for data dependencies hidden
# behind a SQL user-defined function (issue #108713). A SQL UDF is expanded into its body only after the
# referenced-tables collection runs, so a call like `f(x)` looks like an ordinary function and its hidden
# source (a dictGet / joinGet / table read) never reaches the set of referenced tables. The query cache
# must therefore be bypassed instead of serving a result that goes stale when only that source changes.

# The cache key includes the current database, so this test (running in its own database) does not
# need to clear the server-wide query cache (which would require a no-parallel tag). SQL UDFs, however,
# are server-wide, so their names carry the unique database name as a suffix to keep concurrent runs
# of this very test (e.g. under the flaky check) from racing on creating and dropping the same function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

UDF_DICT="udf_dict_lookup_${CLICKHOUSE_DATABASE}"
UDF_JOIN="udf_join_lookup_${CLICKHOUSE_DATABASE}"

CACHE_SETTINGS="use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, query_cache_nondeterministic_function_handling = 'save'"

# A dictionary read hidden behind a SQL UDF. The DIRECT layout reads the source table on every lookup,
# so a change to the source is visible immediately, without a reload.
$CLICKHOUSE_CLIENT --query "
CREATE TABLE dict_source (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO dict_source VALUES (1, 'old');
CREATE DICTIONARY dict (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dict_source')) LAYOUT(DIRECT());
CREATE FUNCTION $UDF_DICT AS (x) -> dictGet('dict', 'v', x);
SELECT $UDF_DICT(toUInt64(1)) SETTINGS $CACHE_SETTINGS;
TRUNCATE TABLE dict_source;
INSERT INTO dict_source VALUES (1, 'new');
-- The fresh value, not the stale cached one.
SELECT $UDF_DICT(toUInt64(1)) SETTINGS $CACHE_SETTINGS;
DROP FUNCTION $UDF_DICT;
DROP DICTIONARY dict;
DROP TABLE dict_source;
"

# A Join table read hidden behind a SQL UDF.
$CLICKHOUSE_CLIENT --query "
CREATE TABLE j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO j VALUES (1, 'old');
CREATE FUNCTION $UDF_JOIN AS (x) -> joinGet('j', 'v', x);
SELECT $UDF_JOIN(toUInt64(1)) SETTINGS $CACHE_SETTINGS;
TRUNCATE TABLE j;
INSERT INTO j VALUES (1, 'new');
-- The fresh value, not the stale cached one.
SELECT $UDF_JOIN(toUInt64(1)) SETTINGS $CACHE_SETTINGS;
DROP FUNCTION $UDF_JOIN;
DROP TABLE j;
"
