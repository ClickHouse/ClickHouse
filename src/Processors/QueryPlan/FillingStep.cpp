#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/Transforms/FillingTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

FillingStep::FillingStep(const DataStream & input_stream_, SortDescription sort_description_)
    : ITransformingStep(input_stream_, input_stream_)
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
