#include <Processors/QueryPlan/TopNAggregatingStep.h>

#include <IO/Operators.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/TopNAggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

TopNAggregatingStep::TopNAggregatingStep(
    const SharedHeader & input_header_,
    Aggregator::Params params_,
    SortDescription sort_description_,
    size_t limit_,
    String order_arg_col_name_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(params_.getHeader(*input_header_, /*final=*/true)),
        getTraits())
    , params(std::move(params_))
    , sort_description(std::move(sort_description_))
    , limit(limit_)
    , order_arg_col_name(std::move(order_arg_col_name_))
{
}

void TopNAggregatingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(params.getHeader(*input_headers.front(), /*final=*/true));
}

void TopNAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto & in_header = *input_headers.front();
    const auto out_header = params.getHeader(in_header, /*final=*/true);

    /// Input is physically sorted by the sort key. Merge N sorted streams into one
    /// before the single-threaded accumulating transform.
    if (pipeline.getNumStreams() > 1 && !order_arg_col_name.empty()
        && in_header.has(order_arg_col_name))
    {
        SortDescription merge_sort_desc;
        SortColumnDescription scd(
            order_arg_col_name,
            sort_description.front().direction,
            sort_description.front().nulls_direction);
        scd.collator = sort_description.front().collator;
        merge_sort_desc.push_back(std::move(scd));

        pipeline.addTransform(std::make_shared<MergingSortedTransform>(
            pipeline.getSharedHeader(),
            pipeline.getNumStreams(),
            merge_sort_desc,
            /*max_block_size=*/DEFAULT_BLOCK_SIZE,
            /*max_block_size_bytes=*/0,
            /*max_dynamic_subcolumns=*/std::nullopt,
            SortingQueueStrategy::Batch,
            /*limit=*/0,
            /*always_read_till_end=*/false,
            /*out_row_sources_buf=*/nullptr,
            /*filter_column_name=*/std::nullopt,
            /*use_average_block_sizes=*/false,
            /*apply_virtual_row_conversions=*/true));
    }
    else
    {
        pipeline.resize(1);
    }

    pipeline.addTransform(std::make_shared<TopNSortedAggregatingTransform>(
        in_header, out_header, params, sort_description, limit));
}

void TopNAggregatingStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, settings.indent_char);
    settings.out << "Keys: ";
    for (size_t i = 0; i < params.keys.size(); ++i)
    {
        if (i > 0)
            settings.out << ", ";
        settings.out << params.keys[i];
    }
    settings.out << '\n';

    settings.out << String(settings.offset, settings.indent_char);
    settings.out << "Limit: " << limit << '\n';

    settings.out << String(settings.offset, settings.indent_char);
    settings.out << "Sorted input: true\n";

    settings.out << String(settings.offset, settings.indent_char);
    settings.out << "Sort description: ";
    for (size_t i = 0; i < sort_description.size(); ++i)
    {
        if (i > 0)
            settings.out << ", ";
        settings.out << sort_description[i].column_name;
        settings.out << (sort_description[i].direction == 1 ? " ASC" : " DESC");
    }
    settings.out << '\n';
}

void TopNAggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto keys_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & key : params.keys)
        keys_array->add(key);
    map.add("Keys", std::move(keys_array));
    map.add("Limit", limit);
    map.add("Sorted Input", true);
}

}
