#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Pipe.h>


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
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    auto block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    auto column_names_and_types = metadata_snapshot->getColumns().getAllWithSubcolumns().addTypes(column_names);

    Columns columns;
    for (const auto & elem : column_names_and_types)
    {
        auto current_column = res_block.getByName(elem.getNameInStorage()).column;
        current_column = current_column->decompress();

        if (elem.isSubcolumn())
            columns.emplace_back(elem.getTypeInStorage()->getSubcolumn(elem.getSubcolumnName(), *current_column));
        else
            columns.emplace_back(std::move(current_column));
    }

    Chunk chunk(std::move(columns), res_block.rows());
    return Pipe(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), std::move(chunk)));
}

}
