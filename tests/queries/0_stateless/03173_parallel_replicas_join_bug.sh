#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -nq "
  CREATE TABLE ids (id UUID, whatever String) Engine=MergeTree ORDER BY tuple();
  INSERT INTO ids VALUES ('a1451105-722e-4fe7-bfaa-65ad2ae249c2', 'whatever');

  CREATE TABLE data (id UUID, event_time DateTime, status String) Engine=MergeTree ORDER BY tuple();
  INSERT INTO data VALUES ('a1451105-722e-4fe7-bfaa-65ad2ae249c2', '2000-01-01', 'CREATED');

  CREATE TABLE data2 (id UUID, event_time DateTime, status String) Engine=MergeTree ORDER BY tuple();
  INSERT INTO data2 VALUES ('a1451105-722e-4fe7-bfaa-65ad2ae249c2', '2000-01-02', 'CREATED');
"

$CLICKHOUSE_CLIENT -nq "
SET enable_analyzer = 1, cluster_for_parallel_replicas = 'parallel_replicas', max_parallel_replicas = 10, allow_experimental_parallel_reading_from_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, max_threads = 1;

SELECT
    id,
    whatever
FROM ids AS l
INNER JOIN view(
    SELECT *
    FROM merge($CLICKHOUSE_DATABASE, 'data.*')
) AS s ON l.id = s.id
WHERE status IN ['CREATED', 'CREATING']
ORDER BY event_time DESC;

SELECT '---------------------------';

with
results1 as (
    SELECT id
    FROM data t1
    inner join ids t2
    on t1.id = t2.id
),
results2 as (
    SELECT id
    FROM ids t1
    inner join data t2
    on t1.id = t2.id
)
select * from results1 union all select * from results2;

SELECT '---------------------------';

with
results1 as (
    SELECT id
    FROM data t1
    inner join ids t2
    on t1.id = t2.id
),
results2 as (
    SELECT id
    FROM ids t1
    inner join data t2
    on t1.id = t2.id
)
select * from results1 t1 inner join results2 t2 using (id);

SELECT '---------------------------';

with
results1 as (
    SELECT t1.id
    FROM data t1
    inner join ids t2 on t1.id = t2.id
    left join data t3 on t2.id = t3.id
),
results2 as (
    SELECT id
    FROM ids t1
    inner join data t2
    on t1.id = t2.id
)
select * from results1 union all select * from results2;
"
