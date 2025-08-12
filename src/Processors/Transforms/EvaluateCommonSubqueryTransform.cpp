#include <Processors/Transforms/EvaluateCommonSubqueryTransform.h>

#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>

namespace DB
{

EvaluateCommonSubqueryTransform::EvaluateCommonSubqueryTransform(
    SharedHeader header_,
    StoragePtr storage_,
    ContextPtr context_
)
    : ISimpleTransform(header_, header_, false)
    , storage(std::move(storage_))
    , context(std::move(context_))
{
    if (!storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage must not be null");
}

void EvaluateCommonSubqueryTransform::transform(Chunk & chunk)
{
    QueryPipelineBuilder builder;
    builder.init(Pipe(std::make_shared<SourceFromSingleChunk>(getInputPort().getSharedHeader(), chunk.clone())));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

    pipeline.complete(storage->write(nullptr, storage->getInMemoryMetadataPtr(), context, /*async_insert=*/false));
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
}

}
