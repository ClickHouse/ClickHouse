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
    StorageBlocks(const std::string & database_name_, const std::string & table_name_,
        const ColumnsDescription & columns_, BlockInputStreams streams_,
        QueryProcessingStage::Enum to_stage_)
        : database_name(database_name_), table_name(table_name_), streams(streams_), to_stage(to_stage_)
    {
        setColumns(columns_);
    }
    static StoragePtr createStorage(const std::string & database_name, const std::string & table_name,
        const ColumnsDescription & columns, BlockInputStreams streams, QueryProcessingStage::Enum to_stage)
    {
        return std::make_shared<StorageBlocks>(database_name, table_name, columns, streams, to_stage);
    }
    std::string getName() const override { return "Blocks"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }
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
    std::string database_name;
    std::string table_name;
    Block res_block;
    BlockInputStreams streams;
    QueryProcessingStage::Enum to_stage;
};

}
