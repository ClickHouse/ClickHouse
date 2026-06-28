#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{

/// Type of index used in EXPLAIN output.
enum class IndexType : uint8_t
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

/// Search algorithm used while resolving primary key ranges, for EXPLAIN output.
/// Mirrors `MarkRanges::SearchAlgorithm`, kept local here so that this header
/// (pulled in transitively through `IQueryPlanStep.h`) does not drag the heavy
/// `MarkRange.h` include into many translation units.
enum class SearchAlgorithm : uint8_t
{
    Unknown,
    BinarySearch,
    GenericExclusionSearch,
};

struct DistributedIndexStat
{
    std::string address;
    size_t num_parts_send = 0;
    size_t num_parts_received = 0;
    size_t num_granules_send = 0;
    size_t num_granules_received = 0;
};

/// Information about an applied index. Used for introspection only, in EXPLAIN query.
struct IndexStat
{
    IndexType type = IndexType::None;
    std::string name = {};
    std::string part_name = {};
    std::string description = {};
    std::string condition = {};
    std::vector<std::string> used_keys = {};
    size_t num_parts_after = 0;
    size_t num_granules_after = 0;
    SearchAlgorithm search_algorithm = {SearchAlgorithm::Unknown};

    std::vector<DistributedIndexStat> distributed = {};
};

using IndexStats = std::vector<IndexStat>;

/// Struct returned by getIndexesDescription for use in EXPLAIN formatting.
struct IndexesDescription
{
    IndexStats index_stats;
    size_t selected_ranges = 0;
    size_t tables_count = 0; /// Non-zero when aggregated from multiple child plans (Merge tables).
};

inline const char * indexTypeToString(IndexType type)
{
    switch (type)
    {
        case IndexType::None:
            return "None";
        case IndexType::MinMax:
            return "Min-Max";
        case IndexType::Partition:
            return "Partition";
        case IndexType::Statistics:
            return "Statistics";
        case IndexType::PrimaryKey:
            return "PrimaryKey";
        case IndexType::Skip:
            return "Skip";
        case IndexType::PrimaryKeyExpand:
            return "PrimaryKeyExpand";
        case IndexType::NonIntersectingSplit:
            return "NonIntersectingSplit";
    }
}

inline std::string_view searchAlgorithmToString(SearchAlgorithm search_algorithm)
{
    switch (search_algorithm)
    {
    case SearchAlgorithm::BinarySearch:
        return "binary search";
    case SearchAlgorithm::GenericExclusionSearch:
        return "generic exclusion search";
    default:
        return "";
    }
}

}
