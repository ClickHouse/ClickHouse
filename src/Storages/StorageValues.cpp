#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

StorageValues::StorageValues(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const Block & res_block_,
    const NamesAndTypesList & virtuals_)
    : IStorage(table_id_), res_block(res_block_), virtuals(virtuals_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageValues::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    storage_snapshot->check(column_names);

    /// Get only required columns.
    Block block;
    for (const auto & name : column_names)
        block.insert(res_block.getByName(name));

    Chunk chunk(block.getColumns(), block.rows());
    return Pipe(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), std::move(chunk)));
}

}
