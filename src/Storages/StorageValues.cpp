#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

StorageValues::StorageValues(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const Block & res_block_,
    VirtualColumnsDescription virtuals_)
    : IStorage(table_id_), res_block(res_block_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(std::move(virtuals_));
}

StorageValues::StorageValues(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    Pipe prepared_pipe_,
    VirtualColumnsDescription virtuals_)
    : IStorage(table_id_), prepared_pipe(std::move(prepared_pipe_))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(std::move(virtuals_));
}

Pipe StorageValues::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    if (!prepared_pipe.empty())
    {
        ActionsDAG dag(prepared_pipe.getHeader().getColumnsWithTypeAndName());
        ActionsDAG::NodeRawConstPtrs outputs;
        outputs.reserve(column_names.size());
        for (const auto & name : column_names)
            outputs.push_back(dag.getOutputs()[prepared_pipe.getHeader().getPositionByName(name)]);

        dag.getOutputs().swap(outputs);
        auto expression = std::make_shared<ExpressionActions>(std::move(dag));

        prepared_pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, expression);
        });

        return std::move(prepared_pipe);
    }

    /// Get only required columns.
    Block block;
    for (const auto & name : column_names)
        block.insert(res_block.getByName(name));

    Chunk chunk(block.getColumns(), block.rows());
    return Pipe(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), std::move(chunk)));
}

}
