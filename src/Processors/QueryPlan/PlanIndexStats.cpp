#include <Processors/QueryPlan/PlanIndexStats.h>

#include <memory>


namespace DB
{

const char * toString(PlanIndexType type)
{
    switch (type)
    {
        case PlanIndexType::None:
            return "None";
        case PlanIndexType::MinMax:
            return "Min-Max";
        case PlanIndexType::Partition:
            return "Partition";
        case PlanIndexType::Statistics:
            return "Statistics";
        case PlanIndexType::PrimaryKey:
            return "PrimaryKey";
        case PlanIndexType::Skip:
            return "Skip";
        case PlanIndexType::PrimaryKeyExpand:
            return "PrimaryKeyExpand";
        case PlanIndexType::NonIntersectingSplit:
            return "NonIntersectingSplit";
    }
}

std::string_view toString(MarkRanges::SearchAlgorithm search_algorithm)
{
    switch (search_algorithm)
    {
        case MarkRanges::SearchAlgorithm::BinarySearch:
            return "binary search";
        case MarkRanges::SearchAlgorithm::GenericExclusionSearch:
            return "generic exclusion search";
        default:
            return "";
    }
}

JSONBuilder::ItemPtr indexStatsToJSON(const PlanIndexStats & index_stats)
{
    if (index_stats.empty())
        return nullptr;

    /// Nothing was applied, so there is nothing to show.
    if (index_stats.size() == 1 && index_stats.front().type == PlanIndexType::None)
        return nullptr;

    auto indexes_array = std::make_unique<JSONBuilder::JSONArray>();

    for (size_t i = 0; i < index_stats.size(); ++i)
    {
        const auto & stat = index_stats[i];
        if (stat.type == PlanIndexType::None)
            continue;

        auto index_map = std::make_unique<JSONBuilder::JSONMap>();

        index_map->add("Type", toString(stat.type));

        if (!stat.name.empty())
            index_map->add("Name", stat.name);

        if (!stat.description.empty())
            index_map->add("Description", stat.description);

        if (!stat.used_keys.empty())
        {
            auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

            for (const auto & used_key : stat.used_keys)
                keys_array->add(used_key);

            index_map->add("Keys", std::move(keys_array));
        }

        if (!stat.condition.empty())
            index_map->add("Condition", stat.condition);

        auto search_algorithm = toString(stat.search_algorithm);
        if (!search_algorithm.empty())
            index_map->add("Search Algorithm", search_algorithm);

        /// What the index started from is what the one before it left, so the first has no "initial".
        if (i)
            index_map->add("Initial Parts", index_stats[i - 1].num_parts_after);
        index_map->add("Selected Parts", stat.num_parts_after);

        if (i)
            index_map->add("Initial Granules", index_stats[i - 1].num_granules_after);
        index_map->add("Selected Granules", stat.num_granules_after);

        if (!stat.distributed.empty())
        {
            auto distributed_index_array = std::make_unique<JSONBuilder::JSONArray>();

            for (const auto & node_stat : stat.distributed)
            {
                auto node_stat_map = std::make_unique<JSONBuilder::JSONMap>();
                node_stat_map->add("Address", node_stat.address);
                node_stat_map->add("Parts send", node_stat.num_parts_send);
                node_stat_map->add("Parts received", node_stat.num_parts_received);
                node_stat_map->add("Granules send", node_stat.num_granules_send);
                node_stat_map->add("Granules received", node_stat.num_granules_received);
                distributed_index_array->add(std::move(node_stat_map));
            }

            index_map->add("Distributed", std::move(distributed_index_array));
        }

        indexes_array->add(std::move(index_map));
    }

    return indexes_array;
}

JSONBuilder::ItemPtr projectionStatsToJSON(const PlanProjectionStats & projection_stats)
{
    if (projection_stats.empty())
        return nullptr;

    auto projections_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & stat : projection_stats)
    {
        auto projection_map = std::make_unique<JSONBuilder::JSONMap>();
        projection_map->add("Name", stat.name);

        if (!stat.description.empty())
            projection_map->add("Description", stat.description);

        if (!stat.condition.empty())
            projection_map->add("Condition", stat.condition);

        auto search_algorithm = toString(stat.search_algorithm);
        if (!search_algorithm.empty())
            projection_map->add("Search Algorithm", search_algorithm);

        projection_map->add("Selected Parts", stat.selected_parts);
        projection_map->add("Selected Marks", stat.selected_marks);
        projection_map->add("Selected Ranges", stat.selected_ranges);
        projection_map->add("Selected Rows", stat.selected_rows);
        projection_map->add("Filtered Parts", stat.filtered_parts);

        projections_array->add(std::move(projection_map));
    }

    return projections_array;
}

}
