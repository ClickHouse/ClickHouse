#!/usr/bin/env bash
# Tags: replica

# In a `Replicated` database (where `POPULATE` requires `database_replicated_allow_heavy_create`) the
# population is always the legacy, non-atomic one: the CREATE is executed as an entry of the replicated
# DDL log, whose metadata transaction is already committed by the creation of the view, so a failed
# atomic population could not roll the view back - `DatabaseReplicated::dropTable` would try to add a
# `ZooKeeper` operation to an already executed transaction, and even a successful unilateral drop would
# diverge this replica from the replicas where the same entry succeeded. This test pins the legacy
# semantics there: a successful `POPULATE` backfills, a failing one reports the error without any
# rollback-induced exception and leaves the view behind (as the legacy population always did), and the
# leftover can be dropped and the CREATE retried.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_db"

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE $DB ENGINE = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/$DB', '{shard}', '{replica}')"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "CREATE TABLE $DB.src (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $DB.src SELECT number FROM numbers(10)"

# POPULATE, allowed with the override, backfills the existing data - through the legacy path.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1 --query "CREATE MATERIALIZED VIEW $DB.mv ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM $DB.src"
${CLICKHOUSE_CLIENT} --query "SELECT 'rows backfilled:', count() FROM $DB.mv"

# A failing population reports its own error - not a logical error from attempting to roll the view back
# through the already committed replicated DDL entry - and leaves the view behind, like the legacy
# population always did.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1 --query "CREATE MATERIALIZED VIEW $DB.mv_failed ENGINE = MergeTree ORDER BY n POPULATE AS SELECT throwIf(n = 5) + n AS n FROM $DB.src" |& grep -cm1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
${CLICKHOUSE_CLIENT} --query "SELECT 'view left behind by the legacy population:', count() FROM system.tables WHERE database = '$DB' AND name = 'mv_failed'"

# The leftover can be dropped and the CREATE retried.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "DROP TABLE $DB.mv_failed"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1 --query "CREATE MATERIALIZED VIEW $DB.mv_failed ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM $DB.src"
${CLICKHOUSE_CLIENT} --query "SELECT 'rows backfilled on retry:', count() FROM $DB.mv_failed"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE $DB"
