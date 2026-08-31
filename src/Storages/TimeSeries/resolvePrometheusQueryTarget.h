#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <optional>


namespace DB
{
class IStorage;

/// A prometheus query target which is a Distributed table over per-shard TimeSeries tables.
struct PrometheusQueryDistributedTarget
{
    String cluster_name;
    StorageID remote_time_series_storage_id = StorageID::createEmpty();
};

/// Checks that a table can be used as a target of a prometheus query, i.e. that it's either a TimeSeries table
/// or a Distributed table over per-shard TimeSeries tables. Returns std::nullopt for a TimeSeries table.
std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage);

/// Refuses what every TimeSeries rewrite silently skips: a non-trivial row policy on the table,
/// and an additional_table_filters entry aimed at it (matched the way the planner matches keys).
void checkTimeSeriesWrapperReadContract(const StorageID & storage_id, const ContextPtr & context);

/// Enforces on the Distributed wrapper what its rewrite skips (SELECT grant, row policies,
/// unmappable settings and filters). Call before reading through a resolved distributed target.
void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context);

}
