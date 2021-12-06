#pragma once

#include <DataTypes/DataTypeDateTime.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>

namespace DB
{

class WindowViewProxyStorage : public IStorage
{
public:
    WindowViewProxyStorage(const StorageID & table_id_, ColumnsDescription columns_, Pipe pipe_, QueryProcessingStage::Enum to_stage_)
    : IStorage(table_id_)
    , pipe(std::move(pipe_))
    , to_stage(to_stage_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_);
        setInMemoryMetadata(storage_metadata);
    }

public:
    std::string getName() const override { return "WindowViewProxy"; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override
    {
        return to_stage;
    }

    Pipe read(
        const Names &,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        return std::move(pipe);
    }

private:
    Pipe pipe;
    QueryProcessingStage::Enum to_stage;
};
}
