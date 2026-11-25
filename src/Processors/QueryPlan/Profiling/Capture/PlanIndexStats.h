#pragma once

#include <Common/DequeWithMemoryTracking.h>
#include <Common/JSONBuilder.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Storages/MergeTree/MarkRange.h>
#include <base/types.h>

#include <cstddef>
#include <deque>
#include <string>
#include <vector>


namespace DB
{

/// What index and projection analysis decided, as data.
///
/// Introspection only: nothing reads these to execute a query, they exist to be shown.

enum class PlanIndexType : uint8_t
{
    None,
    MinMax,
    Partition,
    PrimaryKey,
    Skip,
    PrimaryKeyExpand,
    Statistics,
    NonIntersectingSplit,
};

/// What one replica contributed, when the analysis was distributed.
struct PlanDistributedIndexStat
{
    std::string address = {};
    size_t num_parts_send = 0;
    size_t num_parts_received = 0;
    size_t num_granules_send = 0;
    size_t num_granules_received = 0;
};

struct PlanIndexStat
{
    PlanIndexType type;
    std::string name = {};
    std::string part_name = {};
    std::string description = {};
    std::string condition = {};
    std::vector<std::string> used_keys = {}; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- filled from `KeyCondition::Description::used_keys` and `StatisticsPartPruner::getUsedColumns`
    size_t num_parts_after;
    size_t num_granules_after;
    MarkRanges::SearchAlgorithm search_algorithm = {MarkRanges::SearchAlgorithm::Unknown};

    VectorWithMemoryTracking<PlanDistributedIndexStat> distributed = {};
};

using PlanIndexStats = VectorWithMemoryTracking<PlanIndexStat>;

struct PlanProjectionStat
{
    std::string name = {};
    std::string description = {};
    std::string condition = {};
    MarkRanges::SearchAlgorithm search_algorithm = {MarkRanges::SearchAlgorithm::Unknown};
    UInt64 selected_parts = 0;
    UInt64 selected_ranges = 0;
    UInt64 selected_marks = 0;
    UInt64 selected_rows = 0;
    UInt64 filtered_parts = 0;
};

/// `deque` is used to ensure stable addresses during projection analysis stats building.
using PlanProjectionStats = DequeWithMemoryTracking<PlanProjectionStat>;

const char * toString(PlanIndexType type);
std::string_view toString(MarkRanges::SearchAlgorithm search_algorithm);

/// The `Indexes` and `Projections` entries of a plan document. Null when there is nothing to say,
/// so a caller can skip the key entirely.
/// `with_addresses` is false for the plan stored in `system.query_log.query_plan`, which keeps
/// what each replica contributed but not which replica it was.
JSONBuilder::ItemPtr indexStatsToJSON(const PlanIndexStats & index_stats, bool with_addresses = true);
JSONBuilder::ItemPtr projectionStatsToJSON(const PlanProjectionStats & projection_stats);

}
