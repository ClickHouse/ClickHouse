#include <Storages/ObjectStorage/DataLakes/Iceberg/RowNumbersTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <string>

namespace DB::Iceberg
{

/// Creates a pipeline with RowNumbersTransform added before position delete transforms
void addRowNumbersTransformToPipeline(QueryPipelineBuilder & pipeline)
{
    pipeline.addSimpleTransform([](const Block & header)
    {
        return std::make_shared<RowNumbersTransform>(header.cloneEmpty());
    });
}
