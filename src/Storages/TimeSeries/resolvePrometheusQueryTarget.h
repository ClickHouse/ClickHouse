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
///
/// This is the only decoration the rewrite has to reproduce by hand. A PromQL leaf carries no
/// WHERE/PREWHERE/FINAL/SAMPLE/LIMIT/ORDER BY, so of everything PlannerJoinTree attaches to a
/// TableNode only three items are table-scoped: the SELECT grant (checkAccessRights), the row
/// policy - which for a remote storage the planner refuses rather than applies - and
/// additional_table_filters, which ClusterProxy forwards to the shards itself. Table functions are
/// exempt from the planner's grant check by design, hence this call. If PlannerJoinTree ever grows
/// a fourth decoration keyed on the storage id rather than on a query clause, revisit this.
void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context);

/// The wrapper's declared {skip_unavailable_shards, skip_unavailable_shards_mode}, restated as the
/// generated cluster() call's own declaration so ClusterProxy applies its usual precedence.
std::pair<bool, String> declaredShardSkipSettings(const IStorage & storage, const ContextPtr & context);

}
