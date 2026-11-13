#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <base/types.h>

#include <optional>


namespace DB
{
class Context;
class IStorage;
struct StorageInMemoryMetadata;

/// A prometheus query target which is a Distributed table over per-shard TimeSeries tables.
struct PrometheusQueryDistributedTarget
{
    String cluster_name;
    StorageID remote_time_series_storage_id = StorageID::createEmpty();
    /// The table's declared shard-skip settings, see applyPrometheusQueryDistributedSettings().
    bool skip_unavailable_shards = false;
    String skip_unavailable_shards_mode;
};

/// Checks that a table can be read by a prometheus query, i.e. that it's either a TimeSeries table
/// or a Distributed table over per-shard TimeSeries tables. Returns std::nullopt for a TimeSeries table.
std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage, const ContextPtr & context);

/// The TimeSeries version naming the table's outer samples column: a Distributed table has no version
/// of its own, so it's the one implied by the column it declares.
UInt64 outerSamplesVersion(const IStorage & storage, const StorageInMemoryMetadata & metadata);

/// The query reads the shard-local tables, not the Distributed table itself. So this checks the SELECT grant on it
/// and refuses a row policy or an additional_table_filters entry on it, which would not be applied.
void checkPrometheusQueryDistributedRead(const StorageID & storage_id, const ContextPtr & context);

/// The selector reads the inner tables of a TimeSeries table directly, so a row policy or an additional_table_filters entry
/// on that table would not be applied: refuses them.
void checkTimeSeriesSelectorSource(const StorageID & storage_id, const ContextPtr & context);

/// Prepares the context of the generated query of a Distributed target: the shards get the query text, not a serialized plan
/// that would resolve the shard-local table here, and the table's shard-skip settings apply to the schema discovery
/// of the shards too, unless the query sets them itself.
void applyPrometheusQueryDistributedSettings(Context & query_context, const PrometheusQueryDistributedTarget & target);

}
