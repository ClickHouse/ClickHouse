#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>

/// Walking corpse implementation for removed skipping index of type "hypothesis".
/// Its only purpose is to allow loading old tables with indexes of this type.
/// Data insertion and index usage will throw an exception, suggesting to drop the index.

namespace DB
{

class MergeTreeIndexLegacyHypothesis : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexLegacyHypothesis(const IndexDescription & index_);
    ~MergeTreeIndexLegacyHypothesis() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
};

MergeTreeIndexPtr legacyHypothesisIndexCreator(const IndexDescription & index);
void legacyHypothesisIndexValidator(const IndexDescription & index, bool attach);

}
