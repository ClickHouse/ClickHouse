#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>


namespace DB
{

class MergeTreeMinMaxIndex;


struct MergeTreeMinMaxGranule : public IMergeTreeIndexGranule
{
    explicit MergeTreeMinMaxGranule(const MergeTreeMinMaxIndex & index);
    MergeTreeMinMaxGranule(const MergeTreeMinMaxIndex & index, std::vector<Range> && parallelogram);
    ~MergeTreeMinMaxGranule() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override { return parallelogram.empty(); }

    const MergeTreeMinMaxIndex & index;
    std::vector<Range> parallelogram;
};


struct MergeTreeMinMaxAggregator : IMergeTreeIndexAggregator
{
    explicit MergeTreeMinMaxAggregator(const MergeTreeMinMaxIndex & index);
    ~MergeTreeMinMaxAggregator() override = default;

    bool empty() const override { return parallelogram.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const MergeTreeMinMaxIndex & index;
    std::vector<Range> parallelogram;
};


class MinMaxCondition : public IIndexCondition
{
public:
    MinMaxCondition(
        const SelectQueryInfo & query,
        const Context & context,
        const MergeTreeMinMaxIndex & index);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MinMaxCondition() override = default;
private:
    const MergeTreeMinMaxIndex & index;
    KeyCondition condition;
};


class MergeTreeMinMaxIndex : public IMergeTreeIndex
{
public:
    MergeTreeMinMaxIndex(
        String name_,
        ExpressionActionsPtr expr_,
        const Names & columns_,
        const DataTypes & data_types_,
        const Block & header_,
        size_t granularity_)
        : IMergeTreeIndex(name_, expr_, columns_, data_types_, header_, granularity_) {}

    ~MergeTreeMinMaxIndex() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    IndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;
};

}
