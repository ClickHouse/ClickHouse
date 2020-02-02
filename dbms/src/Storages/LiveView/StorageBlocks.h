#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class StorageBlocks : public IStorage
{
/* Storage based on the prepared streams that already contain data blocks.
 * Used by Live Views to complete stored query based on the mergeable blocks.
 */
public:
    StorageBlocks(const StorageID & table_id_,
        const ColumnsDescription & columns_, BlockInputStreams streams_,
        QueryProcessingStage::Enum to_stage_)
        : IStorage(table_id_), streams(streams_), to_stage(to_stage_)
    {
        setColumns(columns_);
    }
    static StoragePtr createStorage(const StorageID & table_id,
        const ColumnsDescription & columns, BlockInputStreams streams, QueryProcessingStage::Enum to_stage)
    {
        return std::make_shared<StorageBlocks>(table_id, columns, streams, to_stage);
    }
    std::string getName() const override { return "Blocks"; }
    QueryProcessingStage::Enum getQueryProcessingStage(const Context & /*context*/) const override { return to_stage; }

    BlockInputStreams read(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        return streams;
    }

private:
    Block res_block;
    BlockInputStreams streams;
    QueryProcessingStage::Enum to_stage;
};

}
