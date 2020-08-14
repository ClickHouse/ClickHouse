#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/Transforms/FillingTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = false /// TODO: it seem to actually be true. Check it later.
    };
}

FillingStep::FillingStep(const DataStream & input_stream_, SortDescription sort_description_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , sort_description(std::move(sort_description_))
{
}

void FillingStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<FillingTransform>(header, sort_description);
    });
}

}
