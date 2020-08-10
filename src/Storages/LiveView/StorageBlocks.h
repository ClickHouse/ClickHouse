#pragma once

#include <Storages/IStorage.h>
#include <Processors/Pipe.h>


namespace DB
{

class StorageBlocks : public IStorage
{
/* Storage based on the prepared streams that already contain data blocks.
 * Used by Live Views to complete stored query based on the mergeable blocks.
 */
public:
    StorageBlocks(const StorageID & table_id_,
        const ColumnsDescription & columns_, Pipes pipes_,
        QueryProcessingStage::Enum to_stage_)
        : IStorage(table_id_), pipes(std::move(pipes_)), to_stage(to_stage_)
    {
        setColumns(columns_);
    }
    static StoragePtr createStorage(const StorageID & table_id,
        const ColumnsDescription & columns, Pipes pipes, QueryProcessingStage::Enum to_stage)
    {
        return std::make_shared<StorageBlocks>(table_id, columns, std::move(pipes), to_stage);
    }
    std::string getName() const override { return "Blocks"; }
    QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, const ASTPtr &) const override { return to_stage; }

    Pipes read(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        return std::move(pipes);
    }

private:
    Block res_block;
    Pipes pipes;
    QueryProcessingStage::Enum to_stage;
};

}
