#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <memory>
#include <unordered_set>

namespace DB
{

/// MergeTreeIndexUnionCondition implements OR logic between multiple data skipping indexes.
/// If any of the indexes indicates that a granule may contain matching data, the granule is kept.
class MergeTreeIndexUnionCondition : public IMergeTreeIndexMergedCondition
{
public:
    explicit MergeTreeIndexUnionCondition(size_t granularity_)
        : IMergeTreeIndexMergedCondition(granularity_)
    {
    }

    void addIndex(const MergeTreeIndexPtr & index) override;
    void setCondition(size_t index_pos, MergeTreeIndexConditionPtr condition);
    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const override;
    bool mayBeTrueOnGranule(const MergeTreeIndexGranules & granules, const String & part_name) const;
    
    /// Get a description of this union condition
    String getDescription() const;
    
    /// Get the names of all indexes in this union
    std::vector<String> getIndexNames() const;
    
    /// Get detailed statistics about which indexes contributed to filtering
    struct IndexContribution
    {
        String index_name;
        size_t granules_total = 0;
        size_t granules_passed = 0;
        std::unordered_set<String> parts_with_matches;  // Track which parts had matches
    };
    std::vector<IndexContribution> getIndexContributions() const;
    
    /// Enable statistics collection (disabled by default for performance)
    void enableStatistics() { collect_statistics = true; }

private:
    struct IndexWithCondition
    {
        MergeTreeIndexPtr index;
        MergeTreeIndexConditionPtr condition;
    };

    std::vector<IndexWithCondition> indices_with_conditions;
    
    /// Statistics collection
    bool collect_statistics = false;
    mutable std::vector<IndexContribution> index_contributions;
};

} 
