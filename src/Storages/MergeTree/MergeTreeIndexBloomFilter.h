#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexGranuleBloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexAggregatorBloomFilter.h>

namespace DB
{

class MergeTreeIndexBloomFilter : public IMergeTreeIndex
{
public:
    MergeTreeIndexBloomFilter(
        const IndexDescription & index_,
        size_t bits_per_row_,
        size_t hash_functions_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query_info, const Context & context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

private:
    size_t bits_per_row;
    size_t hash_functions;
};

}
