#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


opts=(
    --enable_analyzer=1
    --max_threads=4
)

$CLICKHOUSE_CLIENT -q "
  CREATE TABLE t
  (
    a UInt32
  )
  ENGINE = MergeTree
  ORDER BY a;

  INSERT INTO t SELECT number FROM numbers_mt(1e6);

  OPTIMIZE TABLE t FINAL;
"

query="
	WITH t0 AS
		(
			SELECT *
			FROM numbers(1000)
		)
	SELECT a * 3
	FROM t
	WHERE a IN (t0)
	GROUP BY a
	ORDER BY a
"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "EXPLAIN json=1 $query"

printf "\n\n"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT replaceRegexpAll(explain, '(\w+)\(.*\)', '\\1') FROM (EXPLAIN PIPELINE compact=0,graph=1 $query)"

printf "\n\n"

query_id="03269_explain_unique_ids_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT "${opts[@]}" --log_processors_profiles=1 --query_id="$query_id" --format Null -q "$query"

$CLICKHOUSE_CLIENT -q "
  SYSTEM FLUSH LOGS processors_profile_log;

  SELECT DISTINCT (replaceRegexpAll(processor_uniq_id, '(\w+)\(.*\)', '\\1'), step_uniq_id)
  FROM system.processors_profile_log
  WHERE query_id = '$query_id'
  ORDER BY ALL;
"
