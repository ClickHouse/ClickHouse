#pragma once

#include <Storages/MergeTree/VectorSimilarityCondition.h>

/// Walking corpse implementation for removed skipping index of type "annoy" and "usearch".
/// Its only purpose is to allow loading old tables with indexes of these types.
/// Data insertion and index usage/search will throw an exception, suggesting to migrate to "vector_similarity" indexes.

namespace DB
{

class MergeTreeIndexLegacyVectorSimilarity : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexLegacyVectorSimilarity(const IndexDescription & index_);
    ~MergeTreeIndexLegacyVectorSimilarity() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings &) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo &, ContextPtr) const;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG *, ContextPtr) const override;

    bool isVectorSimilarityIndex() const override { return true; }
};

}
