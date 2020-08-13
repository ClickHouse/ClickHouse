#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Interpreters/SetVariants.h>

#include <memory>
#include <set>


namespace DB
{

class MergeTreeIndexSet;

struct MergeTreeIndexGranuleSet : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleSet(const MergeTreeIndexSet & index_);
    MergeTreeIndexGranuleSet(const MergeTreeIndexSet & index_, MutableColumns && columns_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    size_t size() const { return block.rows(); }
    bool empty() const override { return !size(); }

    ~MergeTreeIndexGranuleSet() override = default;

    const MergeTreeIndexSet & index;
    Block block;
};


struct MergeTreeIndexAggregatorSet : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorSet(const MergeTreeIndexSet & index);
    ~MergeTreeIndexAggregatorSet() override = default;

    size_t size() const { return data.getTotalRowCount(); }
    bool empty() const override { return !size(); }

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    /// return true if has new data
    template <typename Method>
    bool buildFilter(
            Method & method,
            const ColumnRawPtrs & column_ptrs,
            IColumn::Filter & filter,
            size_t pos,
            size_t limit,
            ClearableSetVariants & variants) const;

    const MergeTreeIndexSet & index;
    ClearableSetVariants data;
    Sizes key_sizes;
    MutableColumns columns;
};


class MergeTreeIndexConditionSet : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionSet(
            const SelectQueryInfo & query,
            const Context & context,
            const MergeTreeIndexSet & index);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionSet() override = default;
private:
    void traverseAST(ASTPtr & node) const;
    bool atomFromAST(ASTPtr & node) const;
    static bool operatorFromAST(ASTPtr & node);

    bool checkASTUseless(const ASTPtr & node, bool atomic = false) const;

    const MergeTreeIndexSet & index;

    bool useless;
    std::set<String> key_columns;
    ASTPtr expression_ast;
    ExpressionActionsPtr actions;
};


class MergeTreeIndexSet : public IMergeTreeIndex
{
public:
    MergeTreeIndexSet(
        String name_,
        ExpressionActionsPtr expr_,
        const Names & columns_,
        const DataTypes & data_types_,
        const Block & header_,
        size_t granularity_,
        size_t max_rows_)
        : IMergeTreeIndex(std::move(name_), std::move(expr_), columns_, data_types_, header_, granularity_), max_rows(max_rows_) {}

    ~MergeTreeIndexSet() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, const Context & context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    size_t max_rows = 0;
};

}
