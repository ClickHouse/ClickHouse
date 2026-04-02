#include <Processors/QueryPlan/Optimizations/filterSampling.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Columns/ColumnSet.h>
#include <Columns/FilterDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
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

/// Check whether a DAG contains any not-ready Sets (e.g. from IN subqueries).
static bool hasNotReadySets(const ActionsDAG & dag)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
            continue;
        const auto * column_set = typeid_cast<const ColumnSet *>(node.column.get());
        if (!column_set)
            continue;
        auto future_set = column_set->getData();
        if (!future_set || !future_set->get())
            return true;
    }
    return false;
}

/// Build a `FilterEvaluator` from the filter DAG and/or prewhere info.
/// Returns nullopt if no filter expressions can be extracted, if the DAG
/// contains not-ready Sets, or if required columns are missing from the storage.
/// Populates `columns_to_read` with the columns required by the filters.
static std::optional<FilterEvaluator> buildFilterEvaluator(
    const ActionsDAG * filter_dag,
    const String * filter_column_name,
    const PrewhereInfoPtr & prewhere_info,
    const NameSet & available_columns,
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

    /// Bail out if the filter contains IN subqueries with Sets that haven't been built yet.
    if ((filter_sub_dag && hasNotReadySets(*filter_sub_dag))
        || (prewhere_sub_dag && hasNotReadySets(*prewhere_sub_dag)))
        return std::nullopt;

    NameSet required_columns_set;
    if (filter_sub_dag)
        for (const auto & name : filter_sub_dag->getRequiredColumnsNames())
            required_columns_set.insert(name);
    if (prewhere_sub_dag)
        for (const auto & name : prewhere_sub_dag->getRequiredColumnsNames())
            required_columns_set.insert(name);

    /// Check that all required columns exist in the storage. The filter DAG may reference
    /// qualified names from joins (`__table1.k`) or computed columns (`count()`) that
    /// are not present in the underlying MergeTree table.
    for (const auto & col : required_columns_set)
        if (!available_columns.contains(col))
            return std::nullopt;

    columns_to_read.assign(required_columns_set.begin(), required_columns_set.end());
    if (columns_to_read.empty())
        return std::nullopt;

    FilterEvaluator evaluator;
    if (filter_sub_dag)
    {
        evaluator.filter_actions.emplace(std::move(*filter_sub_dag));
        evaluator.filter_column_name = *filter_column_name;
    }
    if (prewhere_sub_dag)
    {
        evaluator.prewhere_actions.emplace(std::move(*prewhere_sub_dag));
        evaluator.prewhere_column_name = prewhere_info->prewhere_column_name;
    }

    return evaluator;
}

/// Count total granules across all parts and mark ranges.
static size_t countTotalGranules(const RangesInDataParts & parts_with_ranges)
{
    size_t total = 0;
    for (const auto & part : parts_with_ranges)
        for (const auto & range : part.ranges)
            total += range.end - range.begin;
    return total;
}

/// Compute up to `count` evenly-spaced sample positions in [0, total_granules).
/// Each position is placed at the center of its equal-width band for uniform coverage.
static std::vector<size_t> pickEvenlySpacedPositions(size_t total_granules, size_t count)
{
    size_t actual_count = std::min(count, total_granules);
    std::vector<size_t> positions(actual_count);
    for (size_t i = 0; i < actual_count; ++i)
        positions[i] = (2 * i + 1) * total_granules / (2 * actual_count);
    return positions;
}

/// Walk parts and ranges linearly, resolving sorted flat positions to (part, mark) pairs.
static std::vector<std::pair<const RangesInDataPart *, size_t>> resolveGranulePositions(
    const RangesInDataParts & parts_with_ranges,
    const std::vector<size_t> & sorted_positions)
{
    std::vector<std::pair<const RangesInDataPart *, size_t>> result;
    result.reserve(sorted_positions.size());

    size_t flat_offset = 0;
    size_t pos_index = 0;
    for (const auto & part : parts_with_ranges)
    {
        for (const auto & range : part.ranges)
        {
            size_t range_size = range.end - range.begin;
            while (pos_index < sorted_positions.size()
                && sorted_positions[pos_index] < flat_offset + range_size)
            {
                size_t mark = range.begin + (sorted_positions[pos_index] - flat_offset);
                result.emplace_back(&part, mark);
                ++pos_index;
            }
            flat_offset += range_size;
        }
    }
    return result;
}

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
    const String * filter_column_name,
    const ReadFromMergeTree::AnalysisResultPtr & provided_analyzed_result)
{
    try
    {
        auto prewhere_info = read_step.getPrewhereInfo();
        if (!filter_dag && !prewhere_info)
            return std::nullopt;

        auto analyzed_result = provided_analyzed_result;
        if (!analyzed_result)
            analyzed_result = read_step.getAnalyzedResult();
        if (!analyzed_result)
            analyzed_result = read_step.selectRangesToRead();
        if (!analyzed_result || analyzed_result->parts_with_ranges.empty())
            return std::nullopt;

        /// Collect the set of columns available in the storage so we can reject
        /// filter DAGs that reference join-qualified or computed columns.
        const auto & all_columns = read_step.getAllColumnNames();
        NameSet available_columns(all_columns.begin(), all_columns.end());

        Names columns_to_read;
        auto evaluator = buildFilterEvaluator(filter_dag, filter_column_name, prewhere_info, available_columns, columns_to_read);
        if (!evaluator)
            return std::nullopt;

        const auto & parts_with_ranges = analyzed_result->parts_with_ranges;
        size_t total_granules = countTotalGranules(parts_with_ranges);
        if (total_granules == 0)
            return std::nullopt;

        /// Build cache key and check for a cached result.
        String filter_col = filter_column_name ? *filter_column_name : String{};
        String prewhere_col = prewhere_info ? prewhere_info->prewhere_column_name : String{};

        /// Sort columns_to_read so the key is stable regardless of insertion order.
        Names sorted_columns = columns_to_read;
        std::sort(sorted_columns.begin(), sorted_columns.end());

        auto cache_key = FilterSelectivityCache::makeKey(
            read_step.getStorageID(), sorted_columns, filter_col, prewhere_col);

        auto & cache = read_step.getContext()->getFilterSelectivityCache();
        if (auto cached = cache.get(cache_key, total_granules))
        {
            LOG_DEBUG(getLogger("filterSampling"), "Filter selectivity cache hit: {}", *cached);
            return cached;
        }

        /// Use up to 5 samples for reasonable accuracy while keeping I/O minimal.
        /// Each sample reads one granule (~8192 rows). With 5 evenly-spaced samples
        /// the median (3rd value) is robust against up to 2 outlier granules.
        constexpr size_t num_samples = 5;
        auto sample_positions = pickEvenlySpacedPositions(total_granules, num_samples);

        /// Resolve flat positions to (part, mark) pairs and measure selectivity for each.
        auto sampled_granules = resolveGranulePositions(parts_with_ranges, sample_positions);
        std::vector<Float64> selectivities;
        selectivities.reserve(sampled_granules.size());
        for (auto [part, mark] : sampled_granules)
            if (auto selectivity = measureGranuleSelectivity(read_step, *part, mark, columns_to_read, *evaluator))
                selectivities.push_back(*selectivity);

        if (selectivities.empty())
            return std::nullopt;

        std::sort(selectivities.begin(), selectivities.end());
        Float64 median_selectivity = (selectivities.size() % 2 == 1)
            ? selectivities[selectivities.size() / 2]
            : (selectivities[selectivities.size() / 2 - 1] + selectivities[selectivities.size() / 2]) / 2.0;

        if (median_selectivity <= 0.0)
            return std::nullopt;

        cache.put(cache_key, median_selectivity, total_granules);
        return median_selectivity;
    }
    catch (const Exception &)
    {
        tryLogCurrentException(getLogger("filterSampling"), "Failed to estimate filter selectivity", LogsLevel::debug);
        return std::nullopt;
    }
}

}
