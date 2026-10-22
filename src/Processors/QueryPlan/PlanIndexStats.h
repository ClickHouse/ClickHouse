#pragma once

#include <Common/JSONBuilder.h>
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
    std::vector<std::string> used_keys = {};
    size_t num_parts_after;
    size_t num_granules_after;
    MarkRanges::SearchAlgorithm search_algorithm = {MarkRanges::SearchAlgorithm::Unknown};

    std::vector<PlanDistributedIndexStat> distributed = {};
};

using PlanIndexStats = std::vector<PlanIndexStat>;

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
using PlanProjectionStats = std::deque<PlanProjectionStat>;

const char * toString(PlanIndexType type);
std::string_view toString(MarkRanges::SearchAlgorithm search_algorithm);

/// The `Indexes` and `Projections` entries of a plan document. Null when there is nothing to say,
/// so a caller can skip the key entirely.
JSONBuilder::ItemPtr indexStatsToJSON(const PlanIndexStats & index_stats);
JSONBuilder::ItemPtr projectionStatsToJSON(const PlanProjectionStats & projection_stats);

}
