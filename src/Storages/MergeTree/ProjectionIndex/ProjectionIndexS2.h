#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

#include <optional>

namespace DB
{

class ASTProjectionDeclaration;

/// Projection index that accelerates spatial queries on Polygon / MultiPolygon columns
/// using S2 geometry cells.
///
/// SQL syntax:
///   PROJECTION <name> INDEX <polygon_column> TYPE s2(max_cells=8, min_level=8, max_level=16)
///
/// How it works:
///   1. calculate() — During INSERT, each geometry is converted to an S2 bounding-box
///      covering (a set of S2 cell IDs). The projection stores (cell_id, _parent_part_offset)
///      pairs sorted by cell_id. This is a 1:N expansion: one source row produces
///      multiple projection rows (one per covering cell).
///
///   2. tryRewriteFilterForQuery() — At query time, rewrites a predicate like
///        polygonsIntersectSpherical(column, const_polygon)
///      into
///        s2CellsIntersect(cell_id, ancestor_cell)
///      where ancestor_cell is the smallest S2 cell that contains the entire query
///      polygon's covering. This rewritten predicate can be evaluated against the
///      projection's primary key (cell_id) for mark-level pruning.
///
///   3. The generic projection index framework in projectionsCommon.cpp then uses
///      the rewritten filter + _parent_part_offset to identify which rows in the
///      original table actually need to be read.
class ProjectionIndexS2 : public IProjectionIndex
{
public:
    static constexpr auto name = "s2";

    struct Params
    {
        String source_column;       /// The Polygon/MultiPolygon column to index.

        UInt64 max_cells = 8;       /// Maximum number of S2 cells in the covering.
        UInt64 min_level = 16;      /// Minimum S2 cell level (0 = coarsest, 30 = finest).
        UInt64 max_level = 20;      /// Maximum S2 cell level.

        bool strict_decode = false;  /// If true, throw on decode errors; otherwise skip silently.
    };

    /// Factory method called by ProjectionIndexFactory when TYPE = 's2'.
    static ProjectionIndexPtr create(const ASTProjectionDeclaration & proj);

    explicit ProjectionIndexS2(Params params_)
        : params(std::move(params_))
    {
    }

    String getName() const override { return name; }

    const Params & getParams() const { return params; }

    /// Attempt to rewrite a query filter for use with this projection's primary key.
    /// Scans the filter DAG for polygonsIntersectSpherical(source_column, const_geom)
    /// and rewrites it to s2CellsIntersect(cell_id, ancestor_cell).
    /// Returns std::nullopt if no rewrite is possible.
    std::optional<ActionsDAG> tryRewriteFilterForQuery(const ActionsDAG::Node * filter_node, ContextPtr context) const;

    /// Populate the ProjectionDescription metadata: sample_block, sorting/primary key,
    /// column definitions, etc. Called once when the projection is created on a table.
    void fillProjectionDescription(
        ProjectionDescription & result,
        const IAST * index_expr,
        const ColumnsDescription & columns,
        ContextPtr query_context) const override;

    /// Compute the projection block from a source data block. For each row, decodes
    /// the geometry, builds an S2 covering, and emits (cell_id, _parent_part_offset)
    /// pairs. The output is sorted by (cell_id, _parent_part_offset).
    Block calculate(
        const ProjectionDescription & projection_desc,
        const Block & block,
        UInt64 starting_offset,
        ContextPtr context,
        const IColumnPermutation * perm_ptr) const override;

private:
    Params params;
};

}
