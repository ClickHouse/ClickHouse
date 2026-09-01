#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <optional>


namespace DB
{
class IStorage;
class PrometheusQueryTree;

/// A prometheus query target which is a Distributed table over per-shard TimeSeries tables.
struct PrometheusQueryDistributedTarget
{
    String cluster_name;
    StorageID remote_time_series_storage_id = StorageID::createEmpty();
};

/// Checks that a table can be used as a target of a prometheus query, i.e. that it's either a TimeSeries table
/// or a Distributed table over per-shard TimeSeries tables. Returns std::nullopt for a TimeSeries table.
std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage);

/// True when the parsed PromQL contains a selector, i.e. would actually read the table.
bool prometheusQueryReadsTimeSeries(const PrometheusQueryTree & promql_query);

/// Enforces the wrapper's SELECT grant, which its rewrite would otherwise bypass.
/// Call before reading through a resolved distributed target.
void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context);

/// The effective {skip_unavailable_shards, skip_unavailable_shards_mode} of a read through the
/// wrapper, per ClusterProxy rules: declarations are defaults the query overrides.
std::pair<bool, String> effectiveShardSkipSemantics(const IStorage & storage, const ContextPtr & context);

}
