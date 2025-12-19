#pragma once

#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

namespace DB
{

class MergeTreeIndexProjection;

class ProjectionIndexText : public IProjectionIndex
{
public:
    static constexpr auto name = "text";

    static ProjectionIndexPtr create(const ASTProjectionDeclaration & proj);

    explicit ProjectionIndexText(ASTPtr index_ast_)
        : index_ast(std::move(index_ast_))
    {
        sort_description.push_back(SortColumnDescription("term"));
    }

    String getName() const override { return name; }

    void fillProjectionDescription(
        ProjectionDescription & result,
        const IAST * index_expr,
        const ColumnsDescription & columns,
        ContextPtr query_context) const override;

    Block calculate(
        const ProjectionDescription & projection_desc,
        const Block & block,
        UInt64 starting_offset,
        ContextPtr context,
        const IColumnPermutation * perm_ptr) const override;

    std::shared_ptr<MergeTreeSettings> getDefaultSettings() const override;

    const IndexDescription * getIndexDescription() const override;

    MergeTreeIndexPtr getIndex() const override;

    UInt64 getMaxRows() const override { return std::numeric_limits<UInt32>::max(); }

private:
    ASTPtr index_ast;
    SortDescription sort_description;
    IndexDescription index_description;
    std::shared_ptr<const MergeTreeIndexProjection> index;
};

}
