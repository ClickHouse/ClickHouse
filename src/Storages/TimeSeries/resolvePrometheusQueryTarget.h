#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <optional>
#include <string_view>


namespace DB
{
class IStorage;
class PrometheusQueryTree;

/// A prometheus query target which is a Distributed table over per-shard TimeSeries tables.
struct PrometheusQueryDistributedTarget
{
    String cluster_name;
    StorageID remote_time_series_storage_id = StorageID::createEmpty();
    /// The wrapper's declared shard-skip settings, restated on the generated cluster() call.
    bool skip_unavailable_shards = false;
    String skip_unavailable_shards_mode;
};

/// Checks that a table can be used as a target of a prometheus query, i.e. that it's either a TimeSeries table
/// or a Distributed table over per-shard TimeSeries tables. Returns std::nullopt for a TimeSeries table.
std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage);

/// True when the parsed PromQL contains a selector, i.e. would actually read the table.
bool prometheusQueryReadsTimeSeries(const PrometheusQueryTree & promql_query);

/// Refuses `operation`, which reads the table through `rewrite` rather than as itself, while the caller's row policy
/// on the table (unless trivially true) or an additional_table_filters entry aimed at it would be left unapplied.
void checkNoBypassedReadRestriction(
    const StorageID & storage_id, const ContextPtr & context, std::string_view operation, std::string_view rewrite);

/// SELECT on the wrapper, no row policy or filter the rewrite would skip, READ ON REMOTE and a local shard's own grants,
/// all before the probe; then every replica's target must be a TimeSeries table of the wrapper's `time_series` type.
void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context);

/// The same probe for a write, which also refuses an unreachable replica or a missing table or type (it would take
/// samples unchecked) and insert_shard_id / insert_distributed_one_random_shard: the sharding key alone routes a batch.
void checkPrometheusQueryDistributedWrite(const IStorage & storage, const ContextPtr & context);

}
