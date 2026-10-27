#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Iceberg table needs the Parquet and Avro libraries.

# A data lake table keeps its row count in its metadata. The relation estimator reports it, so a
# join with such a table is estimated like one with any other table, and `EXPLAIN estimates` shows
# the rows of the read. Without this the read had no estimate and the join none either.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The path goes into the query as a literal: in a `Replicated` database the `CREATE TABLE` runs
# in the database's own DDL worker, which cannot read anything computed in this session.
ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05344_data_lake_rows_estimate"

sed "s|__ICEBERG_PATH__|${ICEBERG_PATH}|" <<'EOF' | $CLICKHOUSE_CLIENT -mn
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 0;
SET allow_insert_into_iceberg = 1;
SET use_iceberg_metadata_files_cache = 0;
SET use_iceberg_manifest_statistics = 1;
SET iceberg_delete_data_on_drop = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS t_dl_dim;
DROP TABLE IF EXISTS t_dl_fact;
CREATE TABLE t_dl_dim (k Int64, v Int64)
ENGINE = IcebergLocal('__ICEBERG_PATH__', 'Parquet')
SETTINGS iceberg_format_version = 2;
INSERT INTO t_dl_dim SELECT number, number FROM numbers(5);
CREATE TABLE t_dl_fact (k Int64, v Int64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_dl_fact SELECT number % 5, number FROM numbers(100000);

SELECT '-- the lake table reports its 5 rows, and the join is estimated from them';
EXPLAIN estimates = 1 SELECT count() FROM t_dl_fact AS f JOIN t_dl_dim AS d ON f.k = d.k;

SELECT count() FROM t_dl_fact AS f JOIN t_dl_dim AS d ON f.k = d.k;

DROP TABLE t_dl_dim SYNC;
DROP TABLE t_dl_fact;
EOF
