#pragma once

#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

namespace DB
{

/// Projection index that stores specified columns sorted by (_block_number, _block_offset),
/// preserving the original insertion order. Requires enable_block_number_column=1, enable_block_offset_column=1.
///
/// The index expression specifies which columns to store in the projection.
/// _block_number and _block_offset are always appended automatically.
///
/// PROJECTION p INDEX * TYPE commit_order
///   -> SELECT *, _block_number, _block_offset ORDER BY (_block_number, _block_offset)
///
/// PROJECTION p INDEX a, b TYPE commit_order
///   -> SELECT a, b, _block_number, _block_offset ORDER BY (_block_number, _block_offset)
class ProjectionIndexCommitOrder : public IProjectionIndex
{
public:
    static constexpr auto name = "commit_order";

    static ProjectionIndexPtr create(const ASTProjectionDeclaration & /* proj */)
    {
        return std::make_shared<ProjectionIndexCommitOrder>();
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
