#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MergeTreeIndexHyposesis;

class MergeTreeIndexGranuleHypothesis : public IMergeTreeIndexGranule
{
public:
    explicit MergeTreeIndexGranuleHypothesis(
        const String & index_name_);

    MergeTreeIndexGranuleHypothesis(
        const String & index_name_,
        bool met_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return is_empty; }

    ~MergeTreeIndexGranuleHypothesis() override = default;

    const String & index_name;
    bool is_empty = true;
    bool met = true;
};


class MergeTreeIndexAggregatorHypothesis : public IMergeTreeIndexAggregator
{
public:
    explicit MergeTreeIndexAggregatorHypothesis(
        const String & index_name_, const String & column_name_);

    ~MergeTreeIndexAggregatorHypothesis() override = default;

    bool empty() const override { return is_empty; }

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    const String & index_name;
    String column_name;

    bool met = true;
    bool is_empty = true;
};

class MergeTreeIndexHypothesis : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexHypothesis(
        const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexHypothesis() override = default;

    bool isMergeable() const override { return true; }

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG * filter_actions_dag, ContextPtr context) const override;

    MergeTreeIndexMergedConditionPtr createIndexMergedCondition(
        const SelectQueryInfo & query_info, StorageMetadataPtr storage_metadata) const override;

    size_t max_rows = 0;
};

}
