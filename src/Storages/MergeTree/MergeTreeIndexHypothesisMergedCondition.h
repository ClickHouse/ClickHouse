#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/ComparisonGraph.h>

namespace DB
{

/// MergedCondition for Indexhypothesis.
class MergeTreeIndexhypothesisMergedCondition : public IMergeTreeIndexMergedCondition
{
public:
    MergeTreeIndexhypothesisMergedCondition(
        const SelectQueryInfo & query_info, const ConstraintsDescription & constraints, size_t granularity_, ContextPtr context);

    void addIndex(const MergeTreeIndexPtr & index) override;
    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const override;

    ~MergeTreeIndexhypothesisMergedCondition() override;

    class IIndexImpl;
private:
    std::unique_ptr<IIndexImpl> impl{nullptr};
};

}
