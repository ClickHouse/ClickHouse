#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Interpreters/SetVariants.h>

#include <memory>
#include <set>


namespace DB
{

class MergeTreeIndexSet;

struct MergeTreeIndexGranuleSet final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleSet(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t max_rows_);

    MergeTreeIndexGranuleSet(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t max_rows_,
        MutableColumns && columns_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    size_t size() const { return block.rows(); }
    bool empty() const override { return !size(); }

    ~MergeTreeIndexGranuleSet() override = default;

    String index_name;
    size_t max_rows;
    Block index_sample_block;
    Block block;
};


struct MergeTreeIndexAggregatorSet final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorSet(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t max_rows_);

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

    String index_name;
    size_t max_rows;
    Block index_sample_block;

    ClearableSetVariants data;
    Sizes key_sizes;
    MutableColumns columns;
};


class MergeTreeIndexConditionSet final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionSet(
        const String & index_name_,
        const Block & index_sample_block,
        size_t max_rows_,
        const SelectQueryInfo & query_info,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionSet() override = default;
private:
    const ActionsDAG::Node & traverseDAG(const ActionsDAG::Node & node,
        ActionsDAGPtr & result_dag,
        const ContextPtr & context,
        std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> & node_to_result_node) const;

    const ActionsDAG::Node * atomFromDAG(const ActionsDAG::Node & node,
        ActionsDAGPtr & result_dag,
        const ContextPtr & context) const;

    const ActionsDAG::Node * operatorFromDAG(const ActionsDAG::Node & node,
        ActionsDAGPtr & result_dag,
        const ContextPtr & context,
        std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> & node_to_result_node) const;

    bool checkDAGUseless(const ActionsDAG::Node & node, const ContextPtr & context, bool atomic = false) const;

    void traverseAST(ASTPtr & node) const;

    bool atomFromAST(ASTPtr & node) const;

    static bool operatorFromAST(ASTPtr & node);

    bool checkASTUseless(const ASTPtr & node, bool atomic = false) const;

    String index_name;
    size_t max_rows;

    bool isUseless() const
    {
        return actions == nullptr;
    }

    std::unordered_set<String> key_columns;
    ExpressionActionsPtr actions;
};


class MergeTreeIndexSet final : public IMergeTreeIndex
{
public:
    MergeTreeIndexSet(
        const IndexDescription & index_,
        size_t max_rows_)
        : IMergeTreeIndex(index_)
        , max_rows(max_rows_)
    {}

    ~MergeTreeIndexSet() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    size_t max_rows = 0;
};

}
