#include <Processors/Transforms/EvaluateCommonSubqueryTransform.h>

#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

EvaluateCommonSubqueryTransform::EvaluateCommonSubqueryTransform(
    SharedHeader header_,
    SharedHeader common_header_,
    StoragePtr storage_,
    ContextPtr context_,
    const std::vector<size_t> & columns_to_save_indices_
)
    : ISimpleTransform(header_, header_, false)
    , common_header(std::move(common_header_))
    , storage(std::move(storage_))
    , context(std::move(context_))
    , columns_to_save_indices(columns_to_save_indices_)
{
    if (!storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage must not be null");
}

void EvaluateCommonSubqueryTransform::transform(Chunk & chunk)
{
    QueryPipelineBuilder builder;

    Columns columns_to_save;
    columns_to_save.reserve(columns_to_save_indices.size());
    for (size_t index : columns_to_save_indices)
        columns_to_save.push_back(chunk.getColumns()[index]);

    builder.init(Pipe(std::make_shared<SourceFromSingleChunk>(
        common_header,
        Chunk{std::move(columns_to_save), chunk.getNumRows()}
    )));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

    pipeline.complete(storage->write(nullptr, storage->getInMemoryMetadataPtr(), context, /*async_insert=*/false));
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
}

}
