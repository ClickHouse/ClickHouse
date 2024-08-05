#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexGranuleBloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexAggregatorBloomFilter.h>

namespace DB
{

class MergeTreeIndexBloomFilter final : public IMergeTreeIndex
{
public:
    MergeTreeIndexBloomFilter(
        const IndexDescription & index_,
        size_t bits_per_row_,
        size_t hash_functions_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAGPtr & filter_actions_dag, ContextPtr context) const override;

private:
    size_t bits_per_row;
    size_t hash_functions;
};

}
