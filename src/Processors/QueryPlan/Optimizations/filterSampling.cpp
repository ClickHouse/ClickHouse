#include <Processors/QueryPlan/Optimizations/filterSampling.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Columns/FilterDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <algorithm>

namespace DB
{

/// Compiled filter expressions ready to evaluate on blocks.
struct FilterEvaluator
{
    std::optional<ExpressionActions> filter_actions;
    String filter_column_name;
    std::optional<ExpressionActions> prewhere_actions;
    String prewhere_column_name;

    /// Count rows passing all filters in a block (AND semantics).
    size_t countMatching(Block & block) const
    {
        if (prewhere_actions)
            prewhere_actions->execute(block);
        if (filter_actions)
            filter_actions->execute(block);

        if (prewhere_actions && filter_actions)
        {
            FilterDescription prewhere_result(*block.getByName(prewhere_column_name).column);
            FilterDescription filter_result(*block.getByName(filter_column_name).column);
            size_t num_rows = block.rows();
            size_t matching_count = 0;
            for (size_t i = 0; i < num_rows; ++i)
                matching_count += ((*prewhere_result.data)[i] & (*filter_result.data)[i]) ? 1 : 0;
            return matching_count;
        }

        const String & result_column_name = prewhere_actions ? prewhere_column_name : filter_column_name;
        FilterDescription filter_result(*block.getByName(result_column_name).column);
        return filter_result.countBytesInFilter();
    }
};

/// Build a `FilterEvaluator` from the filter DAG and/or prewhere info.
/// Returns nullopt if no filter expressions can be extracted.
/// Populates `columns_to_read` with the columns required by the filters.
static std::optional<FilterEvaluator> buildFilterEvaluator(
    const ActionsDAG * filter_dag,
    const String * filter_column_name,
    const PrewhereInfoPtr & prewhere_info,
    Names & columns_to_read)
{
    auto extract_sub_dag = [](const ActionsDAG & dag, const String & column_name) -> std::optional<ActionsDAG>
    {
        const auto * node = dag.tryFindInOutputs(column_name);
        if (!node)
            return std::nullopt;
        return ActionsDAG::cloneSubDAG({node}, /*remove_aliases=*/ false);
    };

    std::optional<ActionsDAG> filter_sub_dag;
    std::optional<ActionsDAG> prewhere_sub_dag;

    if (filter_dag && filter_column_name)
        filter_sub_dag = extract_sub_dag(*filter_dag, *filter_column_name);
    if (prewhere_info)
        prewhere_sub_dag = extract_sub_dag(prewhere_info->prewhere_actions, prewhere_info->prewhere_column_name);

    if (!filter_sub_dag && !prewhere_sub_dag)
        return std::nullopt;

    NameSet required_columns_set;
    if (filter_sub_dag)
        for (const auto & name : filter_sub_dag->getRequiredColumnsNames())
            required_columns_set.insert(name);
    if (prewhere_sub_dag)
        for (const auto & name : prewhere_sub_dag->getRequiredColumnsNames())
            required_columns_set.insert(name);

    columns_to_read.assign(required_columns_set.begin(), required_columns_set.end());
    if (columns_to_read.empty())
        return std::nullopt;

    FilterEvaluator evaluator;
    if (filter_sub_dag)
        evaluator.filter_actions.emplace(std::move(*filter_sub_dag));
    if (prewhere_sub_dag)
        evaluator.prewhere_actions.emplace(std::move(*prewhere_sub_dag));
    if (filter_column_name)
        evaluator.filter_column_name = *filter_column_name;
    if (prewhere_info)
        evaluator.prewhere_column_name = prewhere_info->prewhere_column_name;

    return evaluator;
}

/// Flat index over granules across all parts and ranges.
/// Builds a prefix-sum array for O(log n) resolution of flat position to (part_index, mark).
class GranuleIndex
{
public:
    explicit GranuleIndex(const RangesInDataParts & parts_with_ranges)
    {
        for (size_t part_index = 0; part_index < parts_with_ranges.size(); ++part_index)
        {
            for (const auto & range : parts_with_ranges[part_index].ranges)
            {
                cumulative_granule_counts.push_back(total_granule_count + (range.end - range.begin));
                part_indices.push_back(part_index);
                range_begins.push_back(range.begin);
                total_granule_count = cumulative_granule_counts.back();
            }
        }
    }

    size_t totalGranules() const { return total_granule_count; }

    /// Pick up to `count` evenly-spaced unique positions across the granule space.
    /// Returns fewer positions when the table has fewer granules than requested.
    std::vector<size_t> pickEvenlySpaced(size_t count) const
    {
        if (total_granule_count == 0)
            return {};

        size_t actual_count = std::min(count, total_granule_count);
        std::vector<size_t> positions(actual_count);
        for (size_t i = 0; i < actual_count; ++i)
            positions[i] = (2 * i + 1) * total_granule_count / (2 * actual_count);
        return positions;
    }

    /// Resolve a flat granule index to (part_index, mark).
    std::pair<size_t, size_t> resolve(size_t flat_position) const
    {
        /// Binary search: find first entry where cumulative count exceeds flat_position.
        size_t range_index = static_cast<size_t>(
            std::upper_bound(cumulative_granule_counts.begin(), cumulative_granule_counts.end(), flat_position)
            - cumulative_granule_counts.begin());
        size_t preceding_granules = range_index > 0 ? cumulative_granule_counts[range_index - 1] : 0;
        return {part_indices[range_index], range_begins[range_index] + (flat_position - preceding_granules)};
    }

private:
    /// One entry per (part, range) pair: cumulative granule count, part index, range start mark.
    std::vector<size_t> cumulative_granule_counts;
    std::vector<size_t> part_indices;
    std::vector<size_t> range_begins;
    size_t total_granule_count = 0;
};

/// Read one granule from a part and return matching_rows / total_rows.
static std::optional<Float64> measureGranuleSelectivity(
    const ReadFromMergeTree & read_step,
    const RangesInDataPart & part,
    size_t mark,
    const Names & columns_to_read,
    const FilterEvaluator & evaluator)
{
    MarkRanges mark_ranges{{mark, mark + 1}};
    auto pipe = createMergeTreeSequentialSource(
        MergeTreeSequentialSourceType::Merge,
        read_step.getMergeTreeData(),
        read_step.getStorageSnapshot(),
        RangesInDataPart(part.data_part, part.parent_part, 0, 0, mark_ranges),
        std::make_shared<AlterConversions>(),
        /*merged_part_offsets=*/ nullptr,
        columns_to_read, mark_ranges,
        /*filtered_rows_count=*/ nullptr,
        /*apply_deleted_mask=*/ false,
        /*read_with_direct_io=*/ false,
        /*prefetch=*/ false);

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    size_t total_rows = 0;
    size_t matching_rows = 0;
    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;
        total_rows += block.rows();
        matching_rows += evaluator.countMatching(block);
    }

    if (total_rows == 0)
        return std::nullopt;
    return Float64(matching_rows) / Float64(total_rows);
}

std::optional<Float64> estimateFilterSelectivity(
    const ReadFromMergeTree & read_step,
    const ActionsDAG * filter_dag,
    const String * filter_column_name)
{
    try
    {
        auto prewhere_info = read_step.getPrewhereInfo();
        if (!filter_dag && !prewhere_info)
            return std::nullopt;

        auto analyzed_result = read_step.getAnalyzedResult();
        if (!analyzed_result)
            analyzed_result = read_step.selectRangesToRead();
        if (!analyzed_result || analyzed_result->parts_with_ranges.empty())
            return std::nullopt;

        Names columns_to_read;
        auto evaluator = buildFilterEvaluator(filter_dag, filter_column_name, prewhere_info, columns_to_read);
        if (!evaluator)
            return std::nullopt;

        GranuleIndex granule_index(analyzed_result->parts_with_ranges);
        if (granule_index.totalGranules() == 0)
            return std::nullopt;

        constexpr size_t num_samples = 3;
        auto sample_positions = granule_index.pickEvenlySpaced(num_samples);

        /// Sample granules and collect selectivities.
        std::vector<Float64> selectivities;
        selectivities.reserve(num_samples);
        for (size_t granule_position : sample_positions)
        {
            auto [part_index, mark] = granule_index.resolve(granule_position);
            if (auto selectivity = measureGranuleSelectivity(
                    read_step, analyzed_result->parts_with_ranges[part_index],
                    mark, columns_to_read, *evaluator))
                selectivities.push_back(*selectivity);
        }

        if (selectivities.empty())
            return std::nullopt;

        std::sort(selectivities.begin(), selectivities.end());
        Float64 median_selectivity = (selectivities.size() % 2 == 1)
            ? selectivities[selectivities.size() / 2]
            : (selectivities[selectivities.size() / 2 - 1] + selectivities[selectivities.size() / 2]) / 2.0;
        return median_selectivity > 0.0 ? std::optional(median_selectivity) : std::nullopt;
    }
    catch (...)
    {
        LOG_DEBUG(getLogger("filterSampling"), "Failed to estimate filter selectivity: {}", getCurrentExceptionMessage(false));
        return std::nullopt;
    }
}

}
