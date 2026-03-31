#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>
#include <Common/logger_useful.h>

#include <optional>

namespace DB
{

class ASTProjectionDeclaration;

/// Projection index that accelerates spatial queries on geometry columns
/// (Point, Ring, LineString, MultiLineString, Polygon, MultiPolygon, Geometry,
///  and their Nullable variants) using S2 cells.
///
/// SQL syntax:
///   PROJECTION <name> INDEX <geometry_column> TYPE s2(max_cells=8, min_level=16, max_level=20)
///
/// How it works:
///   1. calculate() — During INSERT, each geometry is converted to S2 cell IDs:
///      - For Point: one cell ID per row (using S2CellId::FromPoint at max_level)
///      - For Ring/LineString/MultiLineString/Polygon/MultiPolygon: an S2 bounding-box
///        covering (multiple cells per row)
///      The projection stores (cell_id, _parent_part_offset) pairs sorted by cell_id.
///      This is a 1:N expansion: one source row produces one or more projection rows.
///
///   2. tryRewriteFilterForQuery() — At query time, rewrites spatial predicates like
///        ST_Intersects(column, const_polygon)  or  ST_DWithin(column, const_geom, dist)
///      into:
///        __s2CoveringIntersects(cell_id, [c1, c2, ..., cN])
///      The full multi-cell covering is passed as an Array(UInt64) constant.
///      `KeyCondition` recognizes this function and uses `coveringIntersectsRange`
///      to binary-search the sorted `S2CellUnion`, giving much tighter pruning than
///      a single ancestor cell (which could over-cover for spread-out coverings).
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
        String source_column;       /// The geometry column to index (Point/Ring/LineString/MultiLineString/Polygon/MultiPolygon/Geometry, or Nullable).

        UInt64 max_cells = 8;       /// Maximum number of S2 cells in the covering.
        UInt64 min_level = 16;      /// Minimum S2 cell level (0 = coarsest, 30 = finest).
        UInt64 max_level = 20;      /// Maximum S2 cell level.

        bool strict_decode = false;  /// If true, throw on decode errors; otherwise skip silently.
    };

    /// Factory method called by ProjectionIndexFactory when TYPE = 's2'.
    static ProjectionIndexPtr create(const ASTProjectionDeclaration & proj);

    explicit ProjectionIndexS2(Params params_)
        : params(std::move(params_)), log(getLogger("ProjectionIndexS2"))
    {
    }

    String getName() const override { return name; }

    const Params & getParams() const { return params; }

    /// Attempt to rewrite a query filter for use with this projection's primary key.
    /// Scans the filter DAG for spatial predicates on the source column and rewrites
    /// them to `__s2CoveringIntersects(cell_id, covering_array)`.
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
    LoggerPtr log;
};

}
