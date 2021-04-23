#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPipeline.h>
#include <Core/Defines.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{

/// A Storage that allows reading from a single MergeTree data part.
class StorageFromMergeTreeDataPart final : public ext::shared_ptr_helper<StorageFromMergeTreeDataPart>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageFromMergeTreeDataPart>;
public:
    String getName() const override { return "FromMergeTreeDataPart"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams) override
    {
        QueryPlan query_plan =
            std::move(*MergeTreeDataSelectExecutor(parts.front()->storage)
                      .readFromParts(parts, column_names, metadata_snapshot, query_info, context, max_block_size, num_streams, nullptr, &num_granules_from_last_read));

        return query_plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
    }


    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(
        const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & metadata_snapshot) const override
    {
        return parts.front()->storage.mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    NamesAndTypesList getVirtuals() const override
    {
        return parts.front()->storage.getVirtuals();
    }

    String getPartitionId() const
    {
        return parts.front()->info.partition_id;
    }

    String getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr context) const
    {
        return parts.front()->storage.getPartitionIDFromQuery(ast, context);
    }

    size_t getNumGranulesFromLastRead() const { return num_granules_from_last_read; }

protected:
    StorageFromMergeTreeDataPart(const MergeTreeData::DataPartPtr & part_)
        : IStorage(getIDFromPart(part_))
        , parts({part_})
    {
        setInMemoryMetadata(part_->storage.getInMemoryMetadata());
    }

    StorageFromMergeTreeDataPart(MergeTreeData::DataPartsVector && parts_)
        : IStorage(getIDFromParts(parts_))
        , parts(std::move(parts_))
    {
        setInMemoryMetadata(parts.front()->storage.getInMemoryMetadata());
    }

private:
    MergeTreeData::DataPartsVector parts;

    size_t num_granules_from_last_read = 0;

    static StorageID getIDFromPart(const MergeTreeData::DataPartPtr & part_)
    {
        auto table_id = part_->storage.getStorageID();
        return StorageID(table_id.database_name, table_id.table_name + " (part " + part_->name + ")");
    }

    static StorageID getIDFromParts(const MergeTreeData::DataPartsVector & parts_)
    {
        assert(!parts_.empty());
        auto table_id = parts_.front()->storage.getStorageID();
        return StorageID(table_id.database_name, table_id.table_name + " (parts)");
    }
};

}
