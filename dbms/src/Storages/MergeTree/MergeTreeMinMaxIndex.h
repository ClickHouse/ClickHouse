#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

class MergeTreeMinMaxIndex;

struct MergeTreeMinMaxGranule : public MergeTreeIndexGranule
{
    explicit MergeTreeMinMaxGranule(const MergeTreeMinMaxIndex & index);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    String toString() const override;
    bool empty() const override { return parallelogram.empty(); }

    void update(const Block & block, size_t * pos, size_t limit) override;

    ~MergeTreeMinMaxGranule() override = default;

    const MergeTreeMinMaxIndex & index;
    std::vector<Range> parallelogram;
};

class MinMaxCondition : public IndexCondition
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


class MergeTreeMinMaxIndex : public MergeTreeIndex
{
public:
    MergeTreeMinMaxIndex(
        String name,
        ExpressionActionsPtr expr,
        const Names & columns,
        const DataTypes & data_types,
        size_t granularity)
        : MergeTreeIndex(name, expr, columns, data_types, granularity) {}

    ~MergeTreeMinMaxIndex() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    IndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const override;

};

std::unique_ptr<MergeTreeIndex> MergeTreeMinMaxIndexCreator(
    const MergeTreeData & data, std::shared_ptr<ASTIndexDeclaration> node, const Context & context);

}