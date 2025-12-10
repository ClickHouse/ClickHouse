#pragma once

#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

namespace DB
{

class BasicProjectionIndex : public IProjectionIndex
{
public:
    static constexpr auto name = "basic";

    static ProjectionIndexPtr create(const ASTFunction & /* type */)
    {
        return std::make_shared<BasicProjectionIndex>();
    }

    String getName() const override { return "basic"; }

    void fillProjectionDescription(
        ProjectionDescription & result,
        const IAST * index_expr,
        const ColumnsDescription & columns,
        ContextPtr query_context) const override;

    Block
    calculate(const ProjectionDescription & projection_desc, const Block & block, ContextPtr context, const IColumnPermutation * perm_ptr)
        const override;
};

}
