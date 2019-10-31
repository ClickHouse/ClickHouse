#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Core/Defines.h>

#include <ext/shared_ptr_helper.h>
#include <Processors/Executors/TreeExecutorBlockInputStream.h>


namespace DB
{

/// A Storage that allows reading from a single MergeTree data part.
class StorageFromMergeTreeDataPart : public ext::shared_ptr_helper<StorageFromMergeTreeDataPart>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageFromMergeTreeDataPart>;
public:
    String getName() const override { return "FromMergeTreeDataPart"; }
    String getTableName() const override { return part->storage.getTableName() + " (part " + part->name + ")"; }
    String getDatabaseName() const override { return part->storage.getDatabaseName(); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams) override
    {
        auto pipes = MergeTreeDataSelectExecutor(part->storage).readFromParts(
                {part}, column_names, query_info, context, max_block_size, num_streams);

        /// Wrap processors to BlockInputStreams. It is temporary. Will be changed to processors interface later.
        BlockInputStreams streams;
        streams.reserve(pipes.size());

        for (auto & pipe : pipes)
            streams.emplace_back(std::make_shared<TreeExecutorBlockInputStream>(std::move(pipe)));

        return streams;
    }

    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override
    {
        return part->storage.mayBenefitFromIndexForIn(left_in_operand, query_context);
    }

protected:
    StorageFromMergeTreeDataPart(const MergeTreeData::DataPartPtr & part_)
        : IStorage(part_->storage.getVirtuals()), part(part_)
    {
        setColumns(part_->storage.getColumns());
        setIndices(part_->storage.getIndices());
    }

private:
    MergeTreeData::DataPartPtr part;
};

}
