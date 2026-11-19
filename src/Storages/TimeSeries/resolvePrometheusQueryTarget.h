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

/// Checks the SELECT grant and that the table is a TimeSeries table or a Distributed table over them.
/// Returns std::nullopt for a TimeSeries table.
std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage, const ContextPtr & context);

/// The version of the outer samples column. A Distributed table has no version, so it is taken from its column.
UInt64 outerSamplesVersion(const IStorage & storage, const StorageInMemoryMetadata & metadata);

/// The shard-local tables are read, so checks the SELECT grant and refuses a row policy or a filter that would not apply.
void checkPrometheusQueryDistributedRead(const StorageID & storage_id, const ContextPtr & context);

/// Refuses a row policy or a table filter on the source, as the inner tables are read directly.
void checkTimeSeriesSelectorSource(const StorageID & storage_id, const ContextPtr & context);

}
