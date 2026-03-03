#include <Processors/QueryPlan/TopNAggregatingStep.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/Operators.h>
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
    return ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

TopNAggregatingStep::TopNAggregatingStep(
    const SharedHeader & input_header_,
    Names key_names_,
    AggregateDescriptions aggregates_,
    SortDescription sort_description_,
    size_t limit_,
    bool sorted_input_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(buildOutputHeader(*input_header_, key_names_, aggregates_)),
        getTraits())
    , key_names(std::move(key_names_))
    , aggregates(std::move(aggregates_))
    , sort_description(std::move(sort_description_))
    , limit(limit_)
    , sorted_input(sorted_input_)
{
}

void TopNAggregatingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(buildOutputHeader(*input_headers.front(), key_names, aggregates));
}

void TopNAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);

    const auto & in_header = *input_headers.front();
    const auto out_header = buildOutputHeader(in_header, key_names, aggregates);

    pipeline.addTransform(std::make_shared<TopNAggregatingTransform>(
        in_header,
        out_header,
        key_names,
        aggregates,
        sort_description,
        limit,
        sorted_input));
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
}

}
