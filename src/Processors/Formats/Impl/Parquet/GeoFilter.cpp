#include <unordered_set>

#include <Processors/Formats/Impl/Parquet/GeoFilter.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

#include <Common/GeoBbox.h>
#include <Common/logger_useful.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB::Parquet
{

namespace
{

/// Extractor callback for the shared conjunctive collection template.
/// Returns an optional SpatialFilter when the node is a spatial predicate
/// with at least one constant geometry argument.
std::optional<SpatialFilter> tryExtractSpatialFilterFromNode(const ActionsDAG::Node & node, const Block & sample_block)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return std::nullopt;
    if (!node.function_base->isSpatialPredicate() || node.children.size() < 2)
        return std::nullopt;

    const ActionsDAG::Node * col_node = nullptr;
    for (const auto * child : node.children)
        if (child->type == ActionsDAG::ActionType::INPUT && !col_node)
            col_node = child;
    if (!col_node || !sample_block.has(col_node->result_name))
        return std::nullopt;

    double xmin = std::numeric_limits<double>::infinity();
    double ymin = std::numeric_limits<double>::infinity();
    double xmax = -std::numeric_limits<double>::infinity();
    double ymax = -std::numeric_limits<double>::infinity();
    bool found_const = false;
    bool any_extraction_failed = false;

    for (const auto * child : node.children)
    {
        if (child->type != ActionsDAG::ActionType::COLUMN || !child->column
            || !child->is_deterministic_constant)
            continue;
        double cxmin = 0;
        double cymin = 0;
        double cxmax = 0;
        double cymax = 0;
        if (!tryExtractBboxFromColumn(*child->column, cxmin, cymin, cxmax, cymax))
        {
            any_extraction_failed = true;
            continue;
        }
        xmin = std::min(xmin, cxmin);
        ymin = std::min(ymin, cymin);
        xmax = std::max(xmax, cxmax);
        ymax = std::max(ymax, cymax);
        found_const = true;
    }
    /// Fail-closed: if any constant arg could not be converted to a bbox, the union
    /// bbox is incomplete. Pruning on the partial bbox would incorrectly exclude rows
    /// that match only the geometry that was skipped (unsafe for variadic predicates
    /// like `pointInPolygon` where polygon args are OR-ed).
    if (!found_const || any_extraction_failed)
        return std::nullopt;

    SpatialFilter filter;
    filter.geometry_column_name = col_node->result_name;
    filter.query_xmin = xmin;
    filter.query_ymin = ymin;
    filter.query_xmax = xmax;
    filter.query_ymax = ymax;
    return filter;
}

}

std::vector<SpatialFilter> extractSpatialFilters(
    const DB::ActionsDAG & filter_dag,
    const DB::Block & sample_block)
{
    std::vector<SpatialFilter> result;
    std::unordered_set<const DB::ActionsDAG::Node *> visited;

    /// Walk from DAG outputs, following only `and` nodes.
    /// Spatial predicates reachable via OR or other non-conjunctive paths are excluded
    /// because pruning on them would be incorrect (a disjoint spatial branch doesn't mean
    /// the full predicate is false — a non-spatial OR branch could still match).
    for (const auto * output : filter_dag.getOutputs())
        collectSpatialFiltersConjunctive(
            *output, visited,
            [&sample_block](const ActionsDAG::Node & n)
            { return tryExtractSpatialFilterFromNode(n, sample_block); },
            result);

    return result;
}

bool rowGroupFailsSpatialFilters(
    const parq::RowGroup & rg_meta,
    const std::vector<Reader::PrimitiveColumnInfo> & primitive_columns,
    const std::vector<SpatialFilter> & filters)
{
    /// All filters here come from a conjunctive-only extraction (only AND branches).
    /// Therefore: if ANY single filter is disjoint from the row group, the whole AND
    /// conjunction cannot be satisfied — the row group can be pruned.
    for (const auto & filter : filters)
    {
        /// Find the geometry primitive column by name.
        const Reader::PrimitiveColumnInfo * geo_col = nullptr;
        for (const auto & pc : primitive_columns)
        {
            if (pc.name == filter.geometry_column_name)
            {
                geo_col = &pc;
                break;
            }
        }
        if (!geo_col)
            continue;

        /// Only check geospatial_statistics.bbox baked into the geometry column's ColumnMetaData.
        /// covering.bbox is handled via the standard KeyCondition hyperrectangle path.
        if (geo_col->column_idx >= rg_meta.columns.size())
            continue;
        const auto & col_meta = rg_meta.columns[geo_col->column_idx].meta_data;
        if (!col_meta.__isset.geospatial_statistics || !col_meta.geospatial_statistics.__isset.bbox)
            continue;

        const auto & bbox = col_meta.geospatial_statistics.bbox;
        bool disjoint = bbox.xmax < filter.query_xmin
                      || bbox.xmin > filter.query_xmax
                      || bbox.ymax < filter.query_ymin
                      || bbox.ymin > filter.query_ymax;
        if (disjoint)
            return true;
    }

    return false;
}

std::shared_ptr<DB::KeyCondition> buildBboxKeyCondition(
    const SpatialFilter & filter,
    const String & xmin_col, const String & ymin_col,
    const String & xmax_col, const String & ymax_col,
    const DB::ContextPtr & context,
    const DB::Block & extended_sample_block)
{
    for (const auto & name : {xmin_col, ymin_col, xmax_col, ymax_col})
        if (!extended_sample_block.has(name))
            return nullptr;

    auto float64 = std::make_shared<DB::DataTypeFloat64>();
    DB::ActionsDAG dag;

    const auto & xmin_in = dag.addInput(xmin_col, float64);
    const auto & xmax_in = dag.addInput(xmax_col, float64);
    const auto & ymin_in = dag.addInput(ymin_col, float64);
    const auto & ymax_in = dag.addInput(ymax_col, float64);

    auto make_const = [&](double v, const String & name) -> const DB::ActionsDAG::Node &
    {
        return dag.addColumn(DB::ColumnWithTypeAndName{
            float64->createColumnConst(1, DB::Field(v)), float64, name});
    };
    const auto & c_qxmin = make_const(filter.query_xmin, "__bbox_q_xmin");
    const auto & c_qxmax = make_const(filter.query_xmax, "__bbox_q_xmax");
    const auto & c_qymin = make_const(filter.query_ymin, "__bbox_q_ymin");
    const auto & c_qymax = make_const(filter.query_ymax, "__bbox_q_ymax");

    auto & fn = DB::FunctionFactory::instance();
    auto le = fn.get("lessOrEquals", context);
    auto ge = fn.get("greaterOrEquals", context);
    auto and_fn = fn.get("and", context);

    /// bbox_xmin <= query_xmax  AND  bbox_xmax >= query_xmin
    /// bbox_ymin <= query_ymax  AND  bbox_ymax >= query_ymin
    const auto & cmp1 = dag.addFunction(le, {&xmin_in, &c_qxmax}, "");
    const auto & cmp2 = dag.addFunction(ge, {&xmax_in, &c_qxmin}, "");
    const auto & cmp3 = dag.addFunction(le, {&ymin_in, &c_qymax}, "");
    const auto & cmp4 = dag.addFunction(ge, {&ymax_in, &c_qymin}, "");
    const auto & and1 = dag.addFunction(and_fn, {&cmp1, &cmp2}, "");
    const auto & and2 = dag.addFunction(and_fn, {&and1, &cmp3}, "");
    const auto & and3 = dag.addFunction(and_fn, {&and2, &cmp4}, "");
    dag.getOutputs() = {&and3};

    DB::ActionsDAGWithInversionPushDown inverted(dag.getOutputs().front(), context);
    return std::make_shared<DB::KeyCondition>(
        inverted, context, extended_sample_block.getNames(),
        std::make_shared<DB::ExpressionActions>(
            DB::ActionsDAG(extended_sample_block.getColumnsWithTypeAndName())));
}

}
