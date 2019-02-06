#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Interpreters/Set.h>

#include <memory>
#include <set>


namespace DB
{

class MergeTreeUniqueIndex;

struct MergeTreeUniqueGranule : public IMergeTreeIndexGranule
{
    explicit MergeTreeUniqueGranule(const MergeTreeUniqueIndex & index);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    size_t size() const { return set->getTotalRowCount(); }
    bool empty() const override { return !size(); }

    void update(const Block & block, size_t * pos, size_t limit) override;
    Block getElementsBlock() const;

    ~MergeTreeUniqueGranule() override = default;

    const MergeTreeUniqueIndex & index;
    std::unique_ptr<Set> set;
};


class UniqueCondition : public IIndexCondition
{
public:
    UniqueCondition(
            const SelectQueryInfo & query,
            const Context & context,
            const MergeTreeUniqueIndex & index);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~UniqueCondition() override = default;
private:
    void traverseAST(ASTPtr & node) const;
    bool atomFromAST(ASTPtr & node) const;
    bool operatorFromAST(ASTPtr & node) const;

    bool checkASTUseless(const ASTPtr &node, bool atomic = false) const;

    const MergeTreeUniqueIndex & index;

    bool useless;
    std::set<String> key_columns;
    ASTPtr expression_ast;
    ExpressionActionsPtr actions;
};


class MergeTreeUniqueIndex : public IMergeTreeIndex
{
public:
    MergeTreeUniqueIndex(
        String name_,
        ExpressionActionsPtr expr_,
        const Names & columns_,
        const DataTypes & data_types_,
        const Block & header_,
        size_t granularity_,
        size_t max_rows_)
        : IMergeTreeIndex(std::move(name_), std::move(expr_), columns_, data_types_, header_, granularity_), max_rows(max_rows_) {}

    ~MergeTreeUniqueIndex() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    IndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, const Context & context) const override;

    size_t max_rows = 0;
};

std::unique_ptr<IMergeTreeIndex> MergeTreeUniqueIndexCreator(
    const NamesAndTypesList & columns, std::shared_ptr<ASTIndexDeclaration> node, const Context & context);

}
