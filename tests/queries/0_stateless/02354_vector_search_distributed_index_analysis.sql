-- Tags: no-fasttest

set allow_experimental_parallel_reading_from_replicas=0;
set enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
   id UInt32,
   vec Array(Float32),
   INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
Engine=MergeTree
ORDER BY id
SETTINGS index_granularity = 8, distributed_index_analysis_min_parts_to_activate = 0, distributed_index_analysis_min_indexes_bytes_to_activate = 10;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, [number/100, number/100] FROM numbers(100);
INSERT INTO tab SELECT number, [number/100, number/100] FROM numbers(100);
INSERT INTO tab SELECT number, [number/100, number/100] FROM numbers(100);
INSERT INTO tab SELECT number, [number/100, number/100] FROM numbers(100);

SELECT 'KNN results';
SELECT *
FROM tab
ORDER BY L2Distance(vec, [0.3, 0.3]) ASC
LIMIT 4
SETTINGS use_skip_indexes=0;

SELECT 'ANN results';
SELECT *
FROM tab
ORDER BY L2Distance(vec, [0.3, 0.3]) ASC
LIMIT 4;

SELECT 'ANN results - distributed';
SELECT *
FROM tab
ORDER BY L2Distance(vec, [0.3, 0.3]) ASC
LIMIT 4
SETTINGS distributed_index_analysis_for_non_shared_merge_tree = 1, distributed_index_analysis = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas';

-- Common from 03620_distributed_index_analysis.sql
system flush logs query_log;
select format(
  'distributed_index_analysis={}, DistributedIndexAnalysisMicroseconds>0={}, DistributedIndexAnalysisMissingParts={}, DistributedIndexAnalysisScheduledReplicas={}, DistributedIndexAnalysisFailedReplicas>0={}',
  Settings['distributed_index_analysis'],
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0,
  ProfileEvents['DistributedIndexAnalysisMissingParts'],
  ProfileEvents['DistributedIndexAnalysisScheduledReplicas'],
  ProfileEvents['DistributedIndexAnalysisFailedReplicas'] > 0
)
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday()
  and type = 'QueryFinish'
  and query_kind = 'Select'
  and is_initial_query
  and has(Settings, 'distributed_index_analysis')
  and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds;

-- Presence of this metric confirms vector index was used after index analysis
select sum(ProfileEvents['USearchSearchCount']) > 0
from system.query_log
where initial_query_id = (select query_id
        from system.query_log
        where
        current_database = currentDatabase()
        and event_date >= yesterday()
        and type = 'QueryFinish'
        and query_kind = 'Select'
        and is_initial_query
        and has(Settings, 'distributed_index_analysis')
        and endsWith(log_comment, '-' || currentDatabase()));

drop table tab;
