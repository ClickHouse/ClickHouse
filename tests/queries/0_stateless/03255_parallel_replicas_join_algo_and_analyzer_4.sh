#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    item_id UInt64,
    price_sold Float32,
    date Date
)
ENGINE = MergeTree
ORDER BY item_id;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1
(
    item_id UInt64,
    price_sold Float32,
    date Date
)
ENGINE = MergeTree
ORDER BY item_id;

INSERT INTO t SELECT number, number % 10, toDate(number) FROM numbers(100000);
INSERT INTO t1 SELECT number, number % 10, toDate(number) FROM numbers(100000);
"

query1="
  SELECT sum(item_id)
  FROM
  (
      SELECT item_id
      FROM t
      GROUP BY item_id
  ) AS l
  LEFT JOIN
  (
      SELECT item_id
      FROM t1
  ) AS r ON l.item_id = r.item_id
"

query2="
  SELECT sum(item_id)
  FROM
  (
      SELECT item_id
      FROM t
  ) AS l
  LEFT JOIN
  (
      SELECT item_id
      FROM t1
      GROUP BY item_id
  ) AS r ON l.item_id = r.item_id
"

query3="
  SELECT sum(item_id)
  FROM
  (
      SELECT item_id, price_sold
      FROM t
  ) AS l
  LEFT JOIN
  (
      SELECT item_id
      FROM t1
  ) AS r ON l.item_id = r.item_id
  GROUP BY price_sold
  ORDER BY price_sold
"

for parallel_replicas_prefer_local_join in 1 0; do
  for prefer_local_plan in {0..1}; do
    for query in "${query1}" "${query2}" "${query3}"; do
      for enable_parallel_replicas in {0..1}; do
        ${CLICKHOUSE_CLIENT} --query="
        set enable_analyzer=1;
        set parallel_replicas_prefer_local_join=${parallel_replicas_prefer_local_join};
        set parallel_replicas_local_plan=${prefer_local_plan};
        set allow_experimental_parallel_reading_from_replicas=${enable_parallel_replicas}, cluster_for_parallel_replicas='parallel_replicas', max_parallel_replicas=100, parallel_replicas_for_non_replicated_merge_tree=1;

        --SELECT '----- enable_parallel_replicas=$enable_parallel_replicas prefer_local_plan=$prefer_local_plan parallel_replicas_prefer_local_join=$parallel_replicas_prefer_local_join -----';
        ${query};

        SELECT replaceRegexpAll(replaceRegexpAll(explain, '.*Query: (.*) Replicas:.*', '\\1'), '(.*)_data_[\d]+_[\d]+(.*)', '\1_data_x_y_\2') 
        FROM
        (
          EXPLAIN actions=1 ${query}
        )
        WHERE explain LIKE '%ParallelReplicas%';
        "
      done
    done
  done
done
