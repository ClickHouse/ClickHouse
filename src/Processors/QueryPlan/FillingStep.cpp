#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <DataTypes/DataTypeNullable.h>
#include <Processors/Transforms/FillingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Fill values are computed in `Float64`, so a generated value of a narrower float key can round onto an original one.
static bool fillValueMayRound(const Block & header, const String & column_name)
{
    const auto * column = header.findByName(column_name);
    if (!column)
        return true;
    WhichDataType which(removeNullable(column->type));
    return which.isFloat32() || which.isBFloat16();
}

/// A generated row defaults every key that is neither filled nor in the filling sorting prefix. Only such a key ahead of a
/// filled key loses its order: past the last filled key, a generated row already differs from its neighbours on a filled key.
/// A filled key whose generated value may round onto an original one does not separate the rows, so the prefix ends at it.
static size_t preservedSortPrefixSize(
    const Block & header, const SortDescription & sort_description, bool use_with_fill_by_sorting_prefix)
{
    size_t begin = 0;
    if (use_with_fill_by_sorting_prefix)
        while (begin < sort_description.size() && !sort_description[begin].with_fill)
            ++begin;

    size_t last_with_fill = begin;
    for (size_t i = begin; i < sort_description.size(); ++i)
        if (sort_description[i].with_fill)
            last_with_fill = i;

    for (size_t i = begin; i < sort_description.size(); ++i)
    {
        if (!sort_description[i].with_fill)
        {
            if (i < last_with_fill)
                return i;
        }
        else if (fillValueMayRound(header, sort_description[i].column_name))
            return i + 1;
    }

    return sort_description.size();
}

static bool sortingIsPreserved(const Block & header, const SortDescription & sort_description, bool use_with_fill_by_sorting_prefix)
{
    return preservedSortPrefixSize(header, sort_description, use_with_fill_by_sorting_prefix) == sort_description.size();
}

static ITransformingStep::Traits getTraits(bool preserves_sorting)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = preserves_sorting,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FillingStep::FillingStep(
    SharedHeader input_header_,
    SortDescription sort_description_,
    SortDescription fill_description_,
    InterpolateDescriptionPtr interpolate_description_,
    bool use_with_fill_by_sorting_prefix_)
    : ITransformingStep(
          input_header_,
          std::make_shared<const Block>(FillingTransform::transformHeader(*input_header_, sort_description_)),
          getTraits(sortingIsPreserved(*input_header_, sort_description_, use_with_fill_by_sorting_prefix_)))
    , sort_description(std::move(sort_description_))
    , fill_description(std::move(fill_description_))
    , interpolate_description(interpolate_description_)
    , use_with_fill_by_sorting_prefix(use_with_fill_by_sorting_prefix_)
{
}

size_t FillingStep::getPreservedSortPrefixSize() const
{
    return preservedSortPrefixSize(*input_headers.front(), sort_description, use_with_fill_by_sorting_prefix);
}

void FillingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (pipeline.getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FillingStep expects single input");

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return std::make_shared<FillingNoopTransform>(header, fill_description);

        return std::make_shared<FillingTransform>(
            header, sort_description, fill_description, std::move(interpolate_description),
            use_with_fill_by_sorting_prefix, settings.process_list_element);
    });
}

void FillingStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix;
    dumpSortDescription(sort_description, settings);
    settings.out << '\n';
    if (interpolate_description)
    {
        auto expression = std::make_shared<ExpressionActions>(interpolate_description->actions.clone());
        if (!settings.compact)
            expression->describeActions(settings.out, prefix);
    }
}

void FillingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(sort_description));
    if (interpolate_description)
    {
        auto expression = std::make_shared<ExpressionActions>(interpolate_description->actions.clone());
        map.add("Expression", expression->toTree());
    }
}

void FillingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(FillingTransform::transformHeader(*input_headers.front(), sort_description));
}
}
