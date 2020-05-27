#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>


namespace DB
{

struct MergeTreeIndexGranuleMinMax : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleMinMax(const Block & index_sample_block_);
    MergeTreeIndexGranuleMinMax(const Block & index_sample_block_, std::vector<Range> && hyperrectangle_);
    ~MergeTreeIndexGranuleMinMax() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override { return hyperrectangle.empty(); }

    Block index_sample_block;
    std::vector<Range> hyperrectangle;
};


struct MergeTreeIndexAggregatorMinMax : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorMinMax(const Block & index_sample_block);
    ~MergeTreeIndexAggregatorMinMax() override = default;

    bool empty() const override { return hyperrectangle.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    Block index_sample_block;
    std::vector<Range> hyperrectangle;
};


class MergeTreeIndexConditionMinMax : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionMinMax(
        const StorageMetadataSkipIndexField & index,
        const SelectQueryInfo & query,
        const Context & context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionMinMax() override = default;
private:
    DataTypes index_data_types;
    KeyCondition condition;
};


class MergeTreeIndexMinMax : public IMergeTreeIndex
{
public:
    MergeTreeIndexMinMax(const StorageMetadataSkipIndexField & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexMinMax() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;
};

}
