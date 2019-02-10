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

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override { return parallelogram.empty(); }
    void update(const Block & block, size_t * pos, UInt64 limit) override;

    ~MergeTreeMinMaxGranule() override = default;

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

    IndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const override;

};

}
