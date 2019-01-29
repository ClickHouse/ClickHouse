#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Interpreters/Set.h>

#include <memory>


namespace DB
{

class MergeTreeUniqueIndex;

struct MergeTreeUniqueGranule : public MergeTreeIndexGranule
{
    explicit MergeTreeUniqueGranule(const MergeTreeUniqueIndex & index);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    String toString() const override;
    size_t size() const { return set->getTotalRowCount(); }
    bool empty() const override { return !size(); }

    void update(const Block & block, size_t * pos, size_t limit) override;
    Block getElementsBlock() const;

    ~MergeTreeUniqueGranule() override = default;

    const MergeTreeUniqueIndex & index;
    std::unique_ptr<Set> set;
};


class UniqueCondition : public IndexCondition
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

    bool checkASTAlwaysUnknownOrTrue(const ASTPtr & node, bool atomic = false) const;

    const MergeTreeUniqueIndex & index;

    bool useless;
    std::map<String, size_t> key_columns;
    ASTPtr expression_ast;
    ExpressionActionsPtr actions;
};


class MergeTreeUniqueIndex : public MergeTreeIndex
{
public:
    MergeTreeUniqueIndex(
            String name,
            ExpressionActionsPtr expr,
            const Names & columns,
            const DataTypes & data_types,
            const Block & header,
            size_t granularity,
            size_t _max_rows)
            : MergeTreeIndex(name, expr, columns, data_types, header, granularity), max_rows(_max_rows) {}

    ~MergeTreeUniqueIndex() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    IndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, const Context & context) const override;

    size_t max_rows = 0;
};

std::unique_ptr<MergeTreeIndex> MergeTreeUniqueIndexCreator(
        const MergeTreeData & data, std::shared_ptr<ASTIndexDeclaration> node, const Context & context);

}