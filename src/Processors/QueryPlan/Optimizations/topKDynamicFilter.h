#pragma once

#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Common/Logger.h>

#include <functional>
#include <optional>
#include <string_view>

class SipHash;

namespace DB
{

class ITopKThresholdTracker;
using TopKThresholdTrackerPtr = std::shared_ptr<ITopKThresholdTracker>;

namespace QueryPlanOptimizations
{

/// The input column an output of the DAG is a plain copy of, through aliases only (the analyzer renames
/// `a` to `__table1.a` this way); nothing if the output is computed.
std::optional<String> findPassedThroughInput(const ActionsDAG & dag, const String & output_name);

/// A step that keeps the top-`limit` rows or keys by a ranking, and the column the ranking starts with.
struct TopKDynamicFilterConsumer
{
    /// The first ranked column, as named in the output of `node`.
    String column_name;
    size_t limit = 0;
    /// The number of ranked columns, and the direction and the `NULL` direction of each.
    size_t num_ranked_columns = 0;
    const std::vector<int> & directions;
    const std::vector<int> & nulls_directions;

    /// Salts the query condition cache key of the read: a mark of the kind of the consumer, so that its reads never share
    /// entries with the reads of another kind over the same table, and whatever else determines its boundary.
    std::string_view salt_mark;
    std::function<void(SipHash &)> add_salt;
};

/// Links a consumer that publishes the boundary of its first ranked column to the `ReadFromMergeTree` step below `node`,
/// the output of which the consumer reads.
///
/// Once the consumer holds `limit` rows or keys, a row whose first ranked column is beyond the boundary can never reach
/// the result. When that column is a table column that travels to the consumer untouched, the reading step can act on
/// the boundary too, as `ORDER BY column LIMIT n` does with `__topKFilter` (see `tryOptimizeTopK`): a PREWHERE with the
/// running boundary drops such rows before the other columns are read, and the boundary skips whole granules through
/// the primary key or a `minmax` skip index on the column. Returns the tracker the consumer must publish into, or null
/// when the plan shape does not allow it.
///
/// Runs at the end of plan optimization, after PREWHERE was moved and index analysis ran, so unlike `tryOptimizeTopK`
/// it conjoins its filter with the existing PREWHERE and re-salts the query condition cache key of the WHERE filter,
/// which `updateQueryConditionCache` set before the read became a top-K read.
TopKThresholdTrackerPtr tryAttachTopKDynamicFilter(
    QueryPlan::Node * node,
    const TopKDynamicFilterConsumer & consumer,
    const Optimization::ExtraSettings & settings,
    QueryPlan::Nodes & nodes,
    const LoggerPtr & log);

}

}
