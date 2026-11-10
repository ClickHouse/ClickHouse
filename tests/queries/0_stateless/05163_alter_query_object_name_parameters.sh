#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS alter_param_t";
${CLICKHOUSE_CLIENT} --query "CREATE TABLE alter_param_t (a UInt64, b UInt64, c UInt64, INDEX idx a TYPE minmax, PROJECTION prj (SELECT a ORDER BY a), CONSTRAINT cns CHECK a > 0) ENGINE = MergeTree() ORDER BY a";
${CLICKHOUSE_CLIENT} --query "INSERT INTO alter_param_t VALUES (1, 10, 100)";

# RENAME COLUMN with parameterized source and target names.
${CLICKHOUSE_CLIENT} --param_from=b --param_to=b2 --query "ALTER TABLE alter_param_t RENAME COLUMN {from:Identifier} TO {to:Identifier}";
# ADD COLUMN ... AFTER with a parameterized position.
${CLICKHOUSE_CLIENT} --param_after=a --query "ALTER TABLE alter_param_t ADD COLUMN d UInt64 AFTER {after:Identifier}";
# COMMENT COLUMN with a parameterized name.
${CLICKHOUSE_CLIENT} --param_col=c --query "ALTER TABLE alter_param_t COMMENT COLUMN {col:Identifier} 'the c column'";
# DROP COLUMN with a parameterized name.
${CLICKHOUSE_CLIENT} --param_col=c --query "ALTER TABLE alter_param_t DROP COLUMN {col:Identifier}";
# A mutation with a parameterized index name.
${CLICKHOUSE_CLIENT} --param_i=idx --mutations_sync=1 --query "ALTER TABLE alter_param_t MATERIALIZE INDEX {i:Identifier}";
${CLICKHOUSE_CLIENT} --query "SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 'alter_param_t' AND command LIKE '%MATERIALIZE INDEX%'";
# DROP INDEX, PROJECTION and CONSTRAINT with parameterized names.
${CLICKHOUSE_CLIENT} --param_i=idx --param_p=prj --param_c=cns --query "ALTER TABLE alter_param_t DROP INDEX {i:Identifier}, DROP PROJECTION {p:Identifier}, DROP CONSTRAINT {c:Identifier}";
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'alter_param_t'";
${CLICKHOUSE_CLIENT} --query "SELECT countSubstrings(create_table_query, 'PROJECTION') + countSubstrings(create_table_query, 'CONSTRAINT') FROM system.tables WHERE database = currentDatabase() AND name = 'alter_param_t'";

# DROP STATISTICS with a parameterized column name.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE alter_param_t ADD STATISTICS a TYPE minmax";
${CLICKHOUSE_CLIENT} --query "SELECT statistics LIKE '%minmax%' FROM system.columns WHERE database = currentDatabase() AND table = 'alter_param_t' AND name = 'a'";
${CLICKHOUSE_CLIENT} --param_s=a --query "ALTER TABLE alter_param_t DROP STATISTICS {s:Identifier}";
${CLICKHOUSE_CLIENT} --query "SELECT statistics LIKE '%minmax%' FROM system.columns WHERE database = currentDatabase() AND table = 'alter_param_t' AND name = 'a'";

# RENAME b -> b2, ADD d AFTER a, DROP c, so the columns are a, d, b2.
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'alter_param_t' ORDER BY position";

# A parameter used in an object-name position but not provided is rejected.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE alter_param_t DROP COLUMN {unset_col:Identifier}; -- { serverError UNKNOWN_QUERY_PARAMETER }"

${CLICKHOUSE_CLIENT} --query "DROP TABLE alter_param_t";
