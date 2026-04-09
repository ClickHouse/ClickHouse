#include <Processors/QueryPlan/ShuffleStep.h>
#include <Processors/Transforms/ShuffleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits(bool has_limit)
{
    return ITransformingStep::Traits
    {
        {
            /// After resize(1) the pipeline has a single output stream.
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            /// A global shuffle destroys any existing sort order.
            .preserves_sorting = false,
        },
        {
            /// Full shuffle keeps all rows; reservoir sampling may return fewer.
            .preserves_number_of_rows = !has_limit,
        }
    };
}

ShuffleStep::ShuffleStep(const SharedHeader & input_header, std::optional<size_t> limit_)
    : ITransformingStep(input_header, input_header, getTraits(limit_.has_value()))
    , limit(limit_)
{
}

void ShuffleStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// Merge all parallel streams into one so ShuffleTransform sees every row.
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<ShuffleTransform>(header, limit);
    });
}

void ShuffleStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    if (limit)
        settings.out << prefix << "Limit: " << *limit << '\n';
}

void ShuffleStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (limit)
        map.add("Limit", *limit);
}

}
