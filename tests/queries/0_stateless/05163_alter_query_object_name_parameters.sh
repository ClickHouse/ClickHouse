#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS alter_param_t";
${CLICKHOUSE_CLIENT} --query "CREATE TABLE alter_param_t (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree() ORDER BY a";
${CLICKHOUSE_CLIENT} --query "INSERT INTO alter_param_t VALUES (1, 10, 100)";

# RENAME COLUMN with parameterized source and target names.
${CLICKHOUSE_CLIENT} --param_from=b --param_to=b2 --query "ALTER TABLE alter_param_t RENAME COLUMN {from:Identifier} TO {to:Identifier}";
# ADD COLUMN ... AFTER with a parameterized position.
${CLICKHOUSE_CLIENT} --param_after=a --query "ALTER TABLE alter_param_t ADD COLUMN d UInt64 AFTER {after:Identifier}";
# COMMENT COLUMN with a parameterized name.
${CLICKHOUSE_CLIENT} --param_col=c --query "ALTER TABLE alter_param_t COMMENT COLUMN {col:Identifier} 'the c column'";
# DROP COLUMN with a parameterized name.
${CLICKHOUSE_CLIENT} --param_col=c --query "ALTER TABLE alter_param_t DROP COLUMN {col:Identifier}";

# RENAME b -> b2, ADD d AFTER a, DROP c, so the columns are a, d, b2.
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'alter_param_t' ORDER BY position";

# A parameter used in an object-name position but not provided is rejected.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE alter_param_t DROP COLUMN {unset_col:Identifier}; -- { serverError UNKNOWN_QUERY_PARAMETER }"

${CLICKHOUSE_CLIENT} --query "DROP TABLE alter_param_t";
