#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/QueryPlan/PromQLRangeSumByStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Common/logger_useful.h>

#include <optional>

#include <fmt/format.h>


namespace DB
{
namespace QueryPlanOptimizations
{
namespace
{

struct PromQLRangeSumBySource
{
    SortingStep * sorting = nullptr;
    ReadFromMergeTree * reading = nullptr;
};

bool isIDBucketSort(const SortDescription & description)
{
    const auto is_selector_column = [](const String & actual_name, std::string_view expected_name)
    {
        if (actual_name == expected_name)
            return true;

        const auto separator = actual_name.rfind('.');
        return actual_name.starts_with("__table")
            && separator != String::npos
            && std::string_view(actual_name).substr(separator + 1) == expected_name;
    };

    return description.size() == 2
        && is_selector_column(description[0].column_name, TimeSeriesColumnNames::ID)
        && description[0].direction == 1
        && is_selector_column(description[1].column_name, TimeSeriesColumnNames::Bucket)
        && description[1].direction == 1;
}

bool isTransparentUnaryStep(const IQueryPlanStep * step)
{
    return typeid_cast<const ExpressionStep *>(step)
        || typeid_cast<const FilterStep *>(step)
        || typeid_cast<const CreatingSetsStep *>(step)
        || typeid_cast<const DelayedCreatingSetsStep *>(step);
}

std::optional<PromQLRangeSumBySource> findSource(QueryPlan::Node & promql_node, const LoggerPtr & log)
{
    PromQLRangeSumBySource result;
    QueryPlan::Node * node = &promql_node;

    while (node->children.size() == 1)
    {
        node = node->children.front();
        auto * step = node->step.get();

        if (auto * sorting = typeid_cast<SortingStep *>(step))
        {
            if (result.sorting)
            {
                LOG_DEBUG(log, "PromQL source matcher rejected a second SortingStep");
                return std::nullopt;
            }

            if (sorting->getType() != SortingStep::Type::FinishSorting || !isIDBucketSort(sorting->getSortDescription()))
            {
                String description;
                for (const auto & column : sorting->getSortDescription())
                {
                    if (!description.empty())
                        description += ", ";
                    description += fmt::format("{}:{}", column.column_name, column.direction);
                }
                LOG_DEBUG(
                    log,
                    "PromQL source matcher rejected SortingStep (type={}, sort=[{}])",
                    static_cast<unsigned int>(sorting->getType()),
                    description);
                return std::nullopt;
            }
            result.sorting = sorting;
            continue;
        }

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        {
            if (!result.sorting)
            {
                LOG_DEBUG(log, "PromQL source matcher reached ReadFromMergeTree before a supported SortingStep");
                return std::nullopt;
            }
            result.reading = reading;
            return result;
        }

        if (!isTransparentUnaryStep(step))
        {
            LOG_DEBUG(log, "PromQL source matcher rejected step {}", step->getName());
            return std::nullopt;
        }
    }

    return std::nullopt;
}

bool canSplitByID(const ReadFromMergeTree & reading)
{
    if (reading.isQueryWithFinal()
        || reading.isQueryWithSampling()
        || reading.isParallelReadingEnabled()
        || reading.isParallelReadingFromReplicas()
        || reading.willOutputEachPartitionThroughSeparatePort()
        || reading.getNumStreams() <= 1)
        return false;

    const auto & input_order = reading.getInputOrder();
    if (!input_order
        || input_order->direction != 1
        || input_order->used_prefix_of_sorting_key_size < 2)
        return false;

    /// `requestReadingInOrder` stores an empty `sort_description_for_merging` and only retains the
    /// matched sorting-key prefix and direction. The `FinishSorting` step matched by `findSource`
    /// owns the actual `(id, bucket)` description after that request.

    const auto metadata = reading.getStorageMetadata();
    const auto & primary_key = metadata->getPrimaryKey();
    if (primary_key.column_names.empty()
        || primary_key.data_types.empty()
        || primary_key.column_names.front() != TimeSeriesColumnNames::ID
        || !isSafePrimaryDataKeyType(*primary_key.data_types.front()))
        return false;

    if (!primary_key.reverse_flags.empty() && primary_key.reverse_flags.front())
        return false;

    return true;
}

bool tryOptimize(QueryPlan::Node & node, PromQLRangeSumByStep & promql_step)
{
    const auto log = getLogger("optimizePromQLRangeSumByShards");
    if (!promql_step.isParallelProcessingRequested() || promql_step.isParallelProcessingEnabled())
    {
        LOG_DEBUG(
            log,
            "PromQL primary-key range sharding skipped before source analysis (requested={}, enabled={})",
            promql_step.isParallelProcessingRequested(),
            promql_step.isParallelProcessingEnabled());
        return false;
    }

    auto source = findSource(node, log);
    if (!source || !canSplitByID(*source->reading))
    {
        if (!source)
        {
            String path;
            const QueryPlan::Node * current = &node;
            for (size_t depth = 0; current && depth < 16; ++depth)
            {
                if (!path.empty())
                    path += " -> ";
                path += fmt::format("{}[{}]", current->step->getName(), current->children.size());
                current = current->children.size() == 1 ? current->children.front() : nullptr;
            }
            LOG_DEBUG(log, "PromQL primary-key range sharding skipped: source shape is not supported ({})", path);
        }
        else
        {
            const auto & input_order = source->reading->getInputOrder();
            const auto metadata = source->reading->getStorageMetadata();
            const auto & primary_key = metadata->getPrimaryKey();
            LOG_DEBUG(
                log,
                "PromQL primary-key range sharding skipped: read preconditions are not met "
                "(streams={}, final={}, sampling={}, parallel={}, parallel_replicas={}, partition_ports={}, "
                "input_order={}, direction={}, prefix={}, merge_columns={}, primary_key_columns={}, primary_key_first={})",
                source->reading->getNumStreams(),
                source->reading->isQueryWithFinal(),
                source->reading->isQueryWithSampling(),
                source->reading->isParallelReadingEnabled(),
                source->reading->isParallelReadingFromReplicas(),
                source->reading->willOutputEachPartitionThroughSeparatePort(),
                static_cast<bool>(input_order),
                input_order ? input_order->direction : 0,
                input_order ? input_order->used_prefix_of_sorting_key_size : 0,
                input_order ? input_order->sort_description_for_merging.size() : 0,
                primary_key.column_names.size(),
                primary_key.column_names.empty() ? String{"<none>"} : primary_key.column_names.front());
        }
        return false;
    }

    auto analysis_result = source->reading->getAnalyzedResult();
    if (!analysis_result)
        analysis_result = source->reading->selectRangesToRead();

    if (!analysis_result
        || analysis_result->parts_with_ranges.empty()
        || !analysis_result->split_parts.layers.empty())
    {
        LOG_DEBUG(
            log,
            "PromQL primary-key range sharding skipped: selected ranges are unavailable, empty, or already split "
            "(analysis={}, parts={}, existing_layers={})",
            static_cast<bool>(analysis_result),
            analysis_result ? analysis_result->parts_with_ranges.size() : 0,
            analysis_result ? analysis_result->split_parts.layers.size() : 0);
        return false;
    }

    auto split = splitIntersectingPartsRangesIntoLayers(
        analysis_result->parts_with_ranges,
        source->reading->getNumStreams(),
        /*max_columns_in_index=*/1,
        /*in_reverse_order=*/false,
        log);

    if (split.layers.size() <= 1)
    {
        LOG_DEBUG(
            log,
            "PromQL primary-key range sharding skipped: {} selected parts produced only {} layer",
            analysis_result->parts_with_ranges.size(),
            split.layers.size());
        return false;
    }

    LOG_DEBUG(
        log,
        "PromQL primary-key range sharding enabled with {} layers from {} selected parts",
        split.layers.size(),
        analysis_result->parts_with_ranges.size());
    analysis_result->split_parts = std::move(split);
    source->sorting->convertToPartitionedFinishSorting();
    promql_step.enableParallelProcessing();
    return true;
}

}

void optimizePromQLRangeSumByShards(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack{&root};
    while (!stack.empty())
    {
        QueryPlan::Node * node = stack.back();
        stack.pop_back();

        if (auto * promql_step = typeid_cast<PromQLRangeSumByStep *>(node->step.get()))
            tryOptimize(*node, *promql_step);

        stack.insert(stack.end(), node->children.begin(), node->children.end());
    }
}

}
}
