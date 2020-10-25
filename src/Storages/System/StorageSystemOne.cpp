#include <Common/Exception.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemOne.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Pipe.h>


namespace DB
{


StorageSystemOne::StorageSystemOne(const std::string & name_)
    : IStorage({"system", name_})
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({{"dummy", std::make_shared<DataTypeUInt8>()}}));
    setInMemoryMetadata(storage_metadata);
}


Pipes StorageSystemOne::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo &,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    Block header{ColumnWithTypeAndName(
            DataTypeUInt8().createColumn(),
            std::make_shared<DataTypeUInt8>(),
            "dummy")};

    auto column = DataTypeUInt8().createColumnConst(1, 0u)->convertToFullColumnIfConst();
    Chunk chunk({ std::move(column) }, 1);

    Pipes pipes;
    pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));

    return pipes;
}


}
