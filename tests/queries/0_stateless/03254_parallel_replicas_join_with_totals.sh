#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="
CREATE TABLE t
(
    item_id UInt64,
    price_sold Float32,
    date Date
)
ENGINE = MergeTree
ORDER BY item_id;

INSERT INTO t VALUES (1, 100, '1970-01-01'), (1, 200, '1970-01-02');
"

for enable_parallel_replicas in {0..1}; do
  ${CLICKHOUSE_CLIENT} --query="
  -- Plan-based drops the totals block coming from the right side of the join when
  -- 'parallel_replicas_local_plan' is 0: https://github.com/ClickHouse/ClickHouse/issues/120000
  -- Pin the query-based implementation until it is fixed; plan-based gets its own test then.
  set parallel_replicas_plan_based=0;
  set allow_experimental_parallel_reading_from_replicas=${enable_parallel_replicas}, automatic_parallel_replicas_mode=0, cluster_for_parallel_replicas='parallel_replicas', max_parallel_replicas=100, parallel_replicas_for_non_replicated_merge_tree=1;

  SELECT *
  FROM
  (
      SELECT item_id
      FROM t
  ) AS l
  LEFT JOIN
  (
      SELECT item_id
      FROM t
      GROUP BY item_id
          WITH TOTALS
      ORDER BY item_id ASC
  ) AS r ON l.item_id = r.item_id;

  SELECT '-----';
  "
done

${CLICKHOUSE_CLIENT} --query="
DROP TABLE t;
"
