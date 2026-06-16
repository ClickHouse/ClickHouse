#pragma once
#include "config.h"

#if USE_USEARCH

#include <Storages/StorageMergeTreeScoredSearchBase.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Internal temporary storage for the `vectorSearch` table function.
class StorageMergeTreeVectorSearch final : public StorageMergeTreeScoredSearchBase
{
public:
    StorageMergeTreeVectorSearch(
        const StorageID & table_id_,
        StoragePtr source_table_,
        MergeTreeIndexPtr vector_index_,
        VectorWithMemoryTracking<Float64> reference_vector_,
        size_t top_k_,
        const ColumnsDescription & columns,
        const VirtualColumnsDescription & source_virtuals);

    String getName() const override { return "MergeTreeVectorSearch"; }

protected:
    std::shared_ptr<IScorer> createScorer() const override;

    /// Builds the storage virtuals: the scorer-produced `_score`
    /// plus every virtual column of the underlying MergeTree source table.
    static VirtualColumnsDescription createVirtuals(const VirtualColumnsDescription & source_virtuals);

private:
    VectorWithMemoryTracking<Float64> reference_vector;
    size_t top_k;
};

}

#endif
