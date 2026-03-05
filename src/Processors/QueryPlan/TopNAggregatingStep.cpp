#include <Processors/QueryPlan/TopNAggregatingStep.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/Operators.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Port.h>
#include <Processors/Transforms/TopNAggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static Block buildOutputHeader(const Block & input_header, const Names & key_names, const AggregateDescriptions & aggregates)
{
    Block res;
    for (const auto & key : key_names)
        res.insert(input_header.getByName(key).cloneEmpty());

    for (const auto & aggregate : aggregates)
    {
        DataTypePtr type = aggregate.function->getResultType();
        res.insert({type, aggregate.column_name});
    }

    return res;
}

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
    Names key_names_,
    AggregateDescriptions aggregates_,
    SortDescription sort_description_,
    size_t limit_,
    bool sorted_input_,
    bool enable_threshold_pruning_,
    TopKThresholdTrackerPtr threshold_tracker_,
    String order_arg_col_name_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(buildOutputHeader(*input_header_, key_names_, aggregates_)),
        getTraits())
    , key_names(std::move(key_names_))
    , aggregates(std::move(aggregates_))
    , sort_description(std::move(sort_description_))
    , limit(limit_)
    , sorted_input(sorted_input_)
    , enable_threshold_pruning(enable_threshold_pruning_)
    , threshold_tracker(std::move(threshold_tracker_))
    , order_arg_col_name(std::move(order_arg_col_name_))
{
}

void TopNAggregatingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(buildOutputHeader(*input_headers.front(), key_names, aggregates));
}

void TopNAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto & in_header = *input_headers.front();
    const auto out_header = buildOutputHeader(in_header, key_names, aggregates);

    if (sorted_input)
    {
        /// Mode 1: input is physically sorted by the sort key. Merge N sorted
        /// streams into one before the single-threaded accumulating transform.
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
                limit,
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

        pipeline.addTransform(std::make_shared<TopNAggregatingTransform>(
            in_header, out_header, key_names, aggregates, sort_description, limit, sorted_input));
    }
    else
    {
        /// Mode 2: N parallel partial workers (direct HashMap + IAggregateFunction
        /// states), each producing intermediate aggregate state columns, followed
        /// by a single-threaded merge transform. Each worker gets its own reader
        /// stream and benefits from the shared __topKFilter prewhere.
        const auto intermediate_header = buildIntermediateHeader(in_header, key_names, aggregates);

        pipeline.addSimpleTransform([&](const SharedHeader &)
        {
            return std::make_shared<TopNAggregatingTransform>(
                in_header, intermediate_header, key_names, aggregates,
                sort_description, limit, /*sorted_input=*/false, /*partial=*/true,
                enable_threshold_pruning, threshold_tracker);
        });

        pipeline.resize(1);
        pipeline.addTransform(std::make_shared<TopNAggregatingMergeTransform>(
            intermediate_header, out_header, key_names, aggregates, sort_description, limit));
    }
}

void TopNAggregatingStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, settings.indent_char);
    settings.out << "Keys: ";
    for (size_t i = 0; i < key_names.size(); ++i)
    {
        if (i > 0)
            settings.out << ", ";
        settings.out << key_names[i];
    }
    settings.out << '\n';

    settings.out << String(settings.offset, settings.indent_char);
    settings.out << "Limit: " << limit << '\n';

    settings.out << String(settings.offset, settings.indent_char);
    settings.out << "Sorted input: " << (sorted_input ? "true" : "false") << '\n';

    if (enable_threshold_pruning)
    {
        settings.out << String(settings.offset, settings.indent_char);
        settings.out << "Threshold pruning: true\n";
    }

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
    for (const auto & key : key_names)
        keys_array->add(key);
    map.add("Keys", std::move(keys_array));
    map.add("Limit", limit);
    map.add("Sorted Input", sorted_input);
    map.add("Threshold Pruning", enable_threshold_pruning);
}

}
