#pragma once

#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

namespace DB
{

class ProjectionIndexBasic : public IProjectionIndex
{
public:
    static constexpr auto name = "basic";

    static ProjectionIndexPtr create(const ASTProjectionDeclaration & /* proj */)
    {
        return std::make_shared<ProjectionIndexBasic>();
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
};

}
