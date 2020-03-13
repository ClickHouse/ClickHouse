#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>


namespace DB
{

class MergeTreeIndexMinMax;


struct MergeTreeIndexGranuleMinMax : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleMinMax(const MergeTreeIndexMinMax & index_);
    MergeTreeIndexGranuleMinMax(const MergeTreeIndexMinMax & index_, std::vector<Range> && hyperrectangle_);
    ~MergeTreeIndexGranuleMinMax() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override { return hyperrectangle.empty(); }

    const MergeTreeIndexMinMax & index;
    std::vector<Range> hyperrectangle;
};


struct MergeTreeIndexAggregatorMinMax : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorMinMax(const MergeTreeIndexMinMax & index);
    ~MergeTreeIndexAggregatorMinMax() override = default;

    bool empty() const override { return hyperrectangle.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const MergeTreeIndexMinMax & index;
    std::vector<Range> hyperrectangle;
};


class MergeTreeIndexConditionMinMax : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionMinMax(
        const SelectQueryInfo & query,
        const Context & context,
        const MergeTreeIndexMinMax & index);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionMinMax() override = default;
private:
    const MergeTreeIndexMinMax & index;
    KeyCondition condition;
};


class MergeTreeIndexMinMax : public IMergeTreeIndex
{
public:
    MergeTreeIndexMinMax(
        String name_,
        ExpressionActionsPtr expr_,
        const Names & columns_,
        const DataTypes & data_types_,
        const Block & header_,
        size_t granularity_)
        : IMergeTreeIndex(name_, expr_, columns_, data_types_, header_, granularity_) {}

    ~MergeTreeIndexMinMax() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;
};

}
