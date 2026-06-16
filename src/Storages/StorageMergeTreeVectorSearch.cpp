#include "config.h"

#if USE_USEARCH

#include <Storages/StorageMergeTreeVectorSearch.h>
#include <Storages/MergeTree/ScoredSearch/VectorScorer.h>
#include <Storages/VirtualColumnsDescription.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

StorageMergeTreeVectorSearch::StorageMergeTreeVectorSearch(
    const StorageID & table_id_,
    StoragePtr source_table_,
    MergeTreeIndexPtr vector_index_,
    VectorWithMemoryTracking<Float64> reference_vector_,
    size_t top_k_,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & source_virtuals)
    : StorageMergeTreeScoredSearchBase(table_id_, std::move(source_table_), columns, {vector_index_})
    , reference_vector(std::move(reference_vector_))
    , top_k(top_k_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setVirtuals(createVirtuals(source_virtuals));
    setInMemoryMetadata(storage_metadata);
}

std::shared_ptr<IScorer> StorageMergeTreeVectorSearch::createScorer() const
{
    chassert(scorer_indexes.size() == 1);
    return std::make_shared<VectorScorer>(scorer_indexes.at(0), reference_vector, top_k);
}

VirtualColumnsDescription StorageMergeTreeVectorSearch::createVirtuals(const VirtualColumnsDescription & source_virtuals)
{
    VirtualColumnsDescription desc;

    /// `_score` is emitted by the scorer query-plan step, hence `VirtualsMaterializationPlace::Plan`.
    desc.addEphemeral("_score", std::make_shared<DataTypeFloat32>(), "Score for the matched row (USearch raw distance, ASC)", VirtualsMaterializationPlace::Plan);

    /// Expose every virtual column of the underlying MergeTree source table.
    for (const auto & column : source_virtuals)
    {
        /// `_distance` is the source's legacy vector-search reader virtual.
        /// Table function exposes `_distance` as a regular alias for `_score`.
        if (column.name == "_distance")
            continue;

        if (desc.has(column.name))
            continue;

        desc.add(column);
    }

    return desc;
}

}

#endif
