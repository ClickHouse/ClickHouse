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
        StorageInMemoryMetadata metadata_;
        metadata_.setColumns(columns_);
        setInMemoryMetadata(metadata_);
    }
    static StoragePtr createStorage(const StorageID & table_id,
        const ColumnsDescription & columns, Pipes pipes, QueryProcessingStage::Enum to_stage)
    {
        return std::make_shared<StorageBlocks>(table_id, columns, std::move(pipes), to_stage);
    }
    std::string getName() const override { return "Blocks"; }
    /// It is passed inside the query and solved at its level.
    bool supportsPrewhere() const override { return true; }
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override
    {
        return to_stage;
    }

    Pipe read(
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        return Pipe::unitePipes(std::move(pipes));
    }

private:
    Block res_block;
    Pipes pipes;
    QueryProcessingStage::Enum to_stage;
};

}
