#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <optional>
#include <string_view>
#include <vector>


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

/// What one replica said the table the wrapper names on it is: nothing when unreachable, and each field
/// empty when the replica has no such table or does not expose its `time_series` column.
struct PrometheusShardTargetIdentity
{
    String replica;
    bool answered = false;
    String engine;
    String time_series_type;
    String uuid;
    bool operator==(const PrometheusShardTargetIdentity &) const = default;
};

/// The same probe for a write, which also refuses an unreachable replica or a missing table or type (it would take
/// samples unchecked) and insert_shard_id / insert_distributed_one_random_shard: the sharding key alone routes a batch.
std::vector<PrometheusShardTargetIdentity> checkPrometheusQueryDistributedWrite(const IStorage & storage, const ContextPtr & context);

/// The sink writes by name, so a batch is acknowledged only once every replica still holds the target checked: one
/// swapped meanwhile leaves the write's status unknown, a retryable error that has Prometheus resend the batch.
void checkPrometheusQueryDistributedWriteDelivered(
    const IStorage & storage, const ContextPtr & context, const std::vector<PrometheusShardTargetIdentity> & checked_targets);

}
