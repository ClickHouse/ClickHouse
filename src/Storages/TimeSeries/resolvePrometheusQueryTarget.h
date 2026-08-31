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

/// Enforces on the Distributed wrapper what its rewrite skips: the reader's SELECT grant, the
/// absence of a row policy the rewritten query could not apply, and the absence of declared
/// Distributed settings the generated cluster() call could not carry. Call before reading through
/// a resolved distributed target.
void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context);

}
