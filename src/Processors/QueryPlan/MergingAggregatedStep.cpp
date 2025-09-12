#include <Interpreters/Context.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MemoryBoundMerging.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits(bool should_produce_results_in_order_of_bucket_number)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = should_produce_results_in_order_of_bucket_number,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

MergingAggregatedStep::MergingAggregatedStep(
    const Header & input_header_,
    Aggregator::Params params_,
    GroupingSetsParamsList grouping_sets_params_,
    bool final_,
    bool memory_efficient_aggregation_,
    size_t max_threads_,
    size_t memory_efficient_merge_threads_,
    bool should_produce_results_in_order_of_bucket_number_,
    size_t max_block_size_,
    size_t memory_bound_merging_max_block_bytes_,
    bool memory_bound_merging_of_aggregation_results_enabled_)
    : ITransformingStep(
        input_header_,
        MergingAggregatedTransform::appendGroupingIfNeeded(input_header_, params_.getHeader(input_header_, final_)),
        getTraits(should_produce_results_in_order_of_bucket_number_))
    , params(std::move(params_))
    , grouping_sets_params(std::move(grouping_sets_params_))
    , final(final_)
    , memory_efficient_aggregation(memory_efficient_aggregation_)
    , max_threads(max_threads_)
    , memory_efficient_merge_threads(memory_efficient_merge_threads_)
    , max_block_size(max_block_size_)
    , memory_bound_merging_max_block_bytes(memory_bound_merging_max_block_bytes_)
    , should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number_)
    , memory_bound_merging_of_aggregation_results_enabled(memory_bound_merging_of_aggregation_results_enabled_)
{
}

void MergingAggregatedStep::applyOrder(SortDescription input_sort_description)
{
    /// Columns might be reordered during optimization, so we better to update sort description.
    group_by_sort_description = std::move(input_sort_description);
}

void MergingAggregatedStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (memoryBoundMergingWillBeUsed())
    {
        if (input_headers.front().has("__grouping_set") || !grouping_sets_params.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                 "Memory bound merging of aggregated results is not supported for grouping sets.");

        auto transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(), std::move(params), final);
        auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            transform_params,
            group_by_sort_description,
            max_block_size,
            memory_bound_merging_max_block_bytes);

        pipeline.addTransform(std::move(transform));

        /// Do merge of aggregated data in parallel.
        pipeline.resize(max_threads);

        const auto & required_sort_description
            = should_produce_results_in_order_of_bucket_number ? group_by_sort_description : SortDescription{};

        pipeline.addSimpleTransform(
            [&](const Block &) { return std::make_shared<MergingAggregatedBucketTransform>(transform_params, required_sort_description); });

        if (should_produce_results_in_order_of_bucket_number)
        {
            pipeline.addTransform(
                std::make_shared<SortingAggregatedForMemoryBoundMergingTransform>(pipeline.getHeader(), pipeline.getNumStreams()));
        }

        return;
    }

    if (!memory_efficient_aggregation)
    {
        /// We union several sources into one, paralleling the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        auto transform = std::make_shared<MergingAggregatedTransform>(pipeline.getHeader(), params, final, grouping_sets_params, max_threads);
        pipeline.addTransform(std::move(transform));
    }
    else
    {
        if (input_headers.front().has("__grouping_set") || !grouping_sets_params.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                 "Memory efficient merging of aggregated results is not supported for grouping sets.");
        auto num_merge_threads = memory_efficient_merge_threads
                                 ? memory_efficient_merge_threads
                                 : max_threads;

        auto transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(), std::move(params), final);
        pipeline.addMergingAggregatedMemoryEfficientTransform(transform_params, num_merge_threads);
    }

    pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads);
}

void MergingAggregatedStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
    if (!group_by_sort_description.empty())
    {
        String prefix(settings.offset, settings.indent_char);
        settings.out << prefix << "Order: " << dumpSortDescription(group_by_sort_description) << '\n';
    }
}

void MergingAggregatedStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
    if (!group_by_sort_description.empty())
        map.add("Order", dumpSortDescription(group_by_sort_description));
}

void MergingAggregatedStep::updateOutputHeader()
{
    const auto & in_header = input_headers.front();
    output_header = MergingAggregatedTransform::appendGroupingIfNeeded(in_header, params.getHeader(in_header, final));
}

bool MergingAggregatedStep::memoryBoundMergingWillBeUsed() const
{
    return memory_bound_merging_of_aggregation_results_enabled && !group_by_sort_description.empty();
}

const SortDescription & MergingAggregatedStep::getSortDescription() const
{
    if (memoryBoundMergingWillBeUsed() && should_produce_results_in_order_of_bucket_number)
        return group_by_sort_description;

    return IQueryPlanStep::getSortDescription();
}

}
