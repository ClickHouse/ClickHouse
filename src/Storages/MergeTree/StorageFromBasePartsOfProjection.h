#pragma once

#include <Core/Defines.h>
#include <Processors/QueryPipeline.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{
/// A Storage that allows reading from a single MergeTree data part.
class StorageFromBasePartsOfProjection final : public ext::shared_ptr_helper<StorageFromBasePartsOfProjection>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageFromBasePartsOfProjection>;

public:
    String getName() const override { return "FromBasePartsOfProjection"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams) override
    {
        // NOTE: It's used to read normal parts only
        QueryPlan query_plan = std::move(*MergeTreeDataSelectExecutor(storage).readFromParts(
            {},
            column_names,
            metadata_snapshot,
            metadata_snapshot,
            query_info,
            context,
            max_block_size,
            num_streams,
            nullptr,
            query_info.projection ? query_info.projection->merge_tree_data_select_base_cache.get()
                                  : query_info.merge_tree_data_select_cache.get()));

        return query_plan.convertToPipe(
            QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
    }


    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(
        const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & metadata_snapshot) const override
    {
        return storage.mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    NamesAndTypesList getVirtuals() const override { return storage.getVirtuals(); }

protected:
    StorageFromBasePartsOfProjection(const MergeTreeData & storage_, const StorageMetadataPtr & metadata_snapshot)
        : IStorage(storage_.getStorageID()), storage(storage_)
    {
        setInMemoryMetadata(*metadata_snapshot);
    }


private:
    const MergeTreeData & storage;
};

}
