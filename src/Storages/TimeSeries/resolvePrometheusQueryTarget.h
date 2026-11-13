#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <base/types.h>

#include <optional>


namespace DB
{
class IStorage;
struct StorageInMemoryMetadata;

/// A prometheus query target which is a Distributed table over per-shard TimeSeries tables.
struct PrometheusQueryDistributedTarget
{
    String cluster_name;
    StorageID remote_time_series_storage_id = StorageID::createEmpty();
    /// The table's declared shard-skip settings, restated on the generated cluster() call.
    bool skip_unavailable_shards = false;
    String skip_unavailable_shards_mode;
};

/// Checks that a table can be read by a prometheus query, i.e. that it's either a TimeSeries table
/// or a Distributed table over per-shard TimeSeries tables. Returns std::nullopt for a TimeSeries table.
std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage);

/// The TimeSeries version naming the table's outer samples column: a Distributed table has no version
/// of its own, so it's the one implied by the column it declares.
UInt64 outerSamplesVersion(const IStorage & storage, const StorageInMemoryMetadata & metadata);

/// The query reads the shard-local tables, not the Distributed table itself. So this checks the SELECT grant on it
/// and refuses a row policy or an additional_table_filters entry on it, which would not be applied.
void checkPrometheusQueryDistributedRead(const StorageID & storage_id, const ContextPtr & context);

}
