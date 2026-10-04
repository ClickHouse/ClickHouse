-- Totals and extremes of a remote read are emitted by the `Remote` source itself, so the pipeline
-- must not contain the separate `RemoteTotals`/`RemoteExtremes` nodes that used to read the shared
-- `RemoteQueryExecutor` with no ordering against the node still draining its packets.

-- `distributed_group_by_no_merge = 1` is load-bearing, not incidental: the aux ports are only
-- requested when the remote stage is `Complete`, so without it neither carrier below creates an aux
-- port at all and the absence assertions would pass on any binary.

-- Extremes carrier.
SELECT number FROM remote('127.0.0.{1,2}', numbers(4)) ORDER BY number
SETTINGS extremes = 1,
         distributed_group_by_no_merge = 1,
         -- Defaults true, but flipped false to true in the 24.3 block of
         -- `SettingsChangesHistory.cpp`, so a `compatibility` draw below 24.3 would empty the
         -- census below. Pinned per query.
         log_processors_profiles = 1,
         log_comment = '04700_remote_totals_extremes_single_node_extremes';

-- Totals carrier.
SELECT number % 2 AS k, count() FROM remote('127.0.0.{1,2}', numbers(4))
GROUP BY k WITH TOTALS ORDER BY k
SETTINGS distributed_group_by_no_merge = 1,
         log_processors_profiles = 1,
         log_comment = '04700_remote_totals_extremes_single_node_totals';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- `RemoteExtremes` is absent; the other two cells are firing oracles, so the absence cannot be
-- satisfied vacuously. `EmptySink` is the load-bearing one: `uniteExtremes` builds it only where
-- several shards contribute an extremes port, so it reads 0 on exactly the shapes that create no
-- aux port, whereas a `Remote` node exists in those shapes too.
-- Newest matching initial query, not an aggregate over history: CI passes a fixed `--database` in
-- some jobs, so one database serves many runs. `system.processors_profile_log` has no `log_comment`
-- column, hence the resolution through `system.query_log`; these are initiator-side nodes.
SELECT countIf(name = 'RemoteExtremes'),
       countIf(name = 'Remote') > 0,
       countIf(name = 'EmptySink') > 0
FROM system.processors_profile_log
WHERE initial_query_id = (
        SELECT argMax(query_id, event_time_microseconds)
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment = '04700_remote_totals_extremes_single_node_extremes'
          AND type = 'QueryFinish'
          AND is_initial_query
          AND event_date >= yesterday() AND event_time >= now() - 600)
  AND query_id = initial_query_id
  AND event_date >= yesterday() AND event_time >= now() - 600;

-- Same for totals, where `uniteTotals` builds a `Concat` under the identical multi-shard condition.
SELECT countIf(name = 'RemoteTotals'),
       countIf(name = 'Remote') > 0,
       countIf(name = 'Concat') > 0
FROM system.processors_profile_log
WHERE initial_query_id = (
        SELECT argMax(query_id, event_time_microseconds)
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment = '04700_remote_totals_extremes_single_node_totals'
          AND type = 'QueryFinish'
          AND is_initial_query
          AND event_date >= yesterday() AND event_time >= now() - 600)
  AND query_id = initial_query_id
  AND event_date >= yesterday() AND event_time >= now() - 600;
