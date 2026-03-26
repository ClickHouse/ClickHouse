#include <Processors/Formats/Impl/Parquet/GeoFilter.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

#include <Common/logger_useful.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/WKB.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB::Parquet
{

namespace
{

/// Recursively walk a CH column at the given row, adding all 2D points to acc.
/// Handles: ColumnConst (unwrap), ColumnTuple(Float64,Float64) (point),
/// ColumnArray (recurse into elements).
void addFromColumn(DB::BboxAccumulator & acc, const DB::IColumn & col, size_t row)
{
    if (const auto * const_col = typeid_cast<const DB::ColumnConst *>(&col))
    {
        addFromColumn(acc, const_col->getDataColumn(), 0);
        return;
    }
    if (const auto * tuple_col = typeid_cast<const DB::ColumnTuple *>(&col))
    {
        if (tuple_col->tupleSize() < 2 || row >= tuple_col->size())
            return;
        const auto * x_col = typeid_cast<const DB::ColumnFloat64 *>(&tuple_col->getColumn(0));
        const auto * y_col = typeid_cast<const DB::ColumnFloat64 *>(&tuple_col->getColumn(1));
        if (!x_col || !y_col)
            return;
        acc.add(x_col->getData()[row], y_col->getData()[row]);
        return;
    }
    if (const auto * array_col = typeid_cast<const DB::ColumnArray *>(&col))
    {
        if (row >= array_col->size())
            return;
        const auto & offsets = array_col->getOffsets();
        const size_t start = row > 0 ? offsets[row - 1] : 0;
        const size_t end = offsets[row];
        for (size_t i = start; i < end; ++i)
            addFromColumn(acc, array_col->getData(), i);
    }
}

bool tryExtractWkbBbox(std::string_view wkb,
                       double & xmin, double & ymin,
                       double & xmax, double & ymax)
{
    DB::ReadBufferFromMemory buf(wkb);
    DB::BboxAccumulator acc;
    try
    {
        auto geo = DB::parseWKBFormat(buf);
        std::visit([&]<typename T>(const T & g)
        {
            if constexpr (std::is_same_v<T, DB::CartesianPoint>)
                acc.add(g.x(), g.y());
            else if constexpr (std::is_same_v<T, DB::LineString<DB::CartesianPoint>>)
                acc.addAll(g);
            else if constexpr (std::is_same_v<T, DB::Polygon<DB::CartesianPoint>>)
                acc.addAll(g.outer());
            else if constexpr (std::is_same_v<T, DB::MultiLineString<DB::CartesianPoint>>)
                for (const auto & ls : g) acc.addAll(ls);
            else if constexpr (std::is_same_v<T, DB::MultiPolygon<DB::CartesianPoint>>)
                for (const auto & poly : g) acc.addAll(poly.outer());
            else
                static_assert(!sizeof(T), "Unhandled geometry type — add a case here");
        }, geo);
    }
    catch (...)
    {
        LOG_TRACE(getLogger("GeoFilter"), "Failed to parse WKB geometry for bbox extraction: {}", getCurrentExceptionMessage(false));
        return false;
    }

    if (!acc.found) return false;
    xmin = acc.xmin; ymin = acc.ymin;
    xmax = acc.xmax; ymax = acc.ymax;
    return true;
}

/// Try to extract the bounding box of a constant ActionsDAG node.
/// Handles WKB-encoded String (st_geomfromgeojson / st_geomfromtext constants)
/// and CH native geometry (ColumnTuple / ColumnArray).
bool tryExtractConstBbox(
    const DB::ActionsDAG::Node * node,
    double & xmin, double & ymin,
    double & xmax, double & ymax)
{
    if (!node->column || !node->is_deterministic_constant)
        return false;

    const DB::IColumn * raw = node->column.get();
    if (const auto * const_col = typeid_cast<const DB::ColumnConst *>(raw))
        raw = &const_col->getDataColumn();

    /// WKB-encoded String (e.g., constant from st_geomfromgeojson / st_geomfromtext).
    if (const auto * str_col = typeid_cast<const DB::ColumnString *>(raw))
    {
        if (str_col->size() == 0) return false;
        return tryExtractWkbBbox(str_col->getDataAt(0), xmin, ymin, xmax, ymax);
    }

    /// CH native geometry (Tuple of floats, Array of Tuples, etc.).
    DB::BboxAccumulator acc;
    addFromColumn(acc, *raw, 0);
    if (!acc.found) return false;
    xmin = acc.xmin; ymin = acc.ymin;
    xmax = acc.xmax; ymax = acc.ymax;
    return true;
}


} // namespace

std::vector<SpatialFilter> extractSpatialFilters(
    const DB::ActionsDAG & filter_dag,
    const DB::Block & sample_block)
{
    std::vector<SpatialFilter> result;

    for (const auto & node : filter_dag.getNodes())
    {
        if (node.type != DB::ActionsDAG::ActionType::FUNCTION)
            continue;
        if (!node.function_base)
            continue;

        if (!node.function_base->isSpatialPredicate())
            continue;
        if (node.children.size() < 2)
            continue;

        /// Find one INPUT child (the geometry column) and one constant COLUMN child (geometry constant).
        const DB::ActionsDAG::Node * col_node = nullptr;
        const DB::ActionsDAG::Node * const_node = nullptr;

        for (const auto * child : node.children)
        {
            if (child->type == DB::ActionsDAG::ActionType::INPUT && !col_node)
                col_node = child;
            else if (child->type == DB::ActionsDAG::ActionType::COLUMN
                     && child->column
                     && child->is_deterministic_constant
                     && !const_node)
                const_node = child;
        }

        if (!col_node || !const_node)
            continue;

        /// The column must be in the sample block (it's a real data column, not a prewhere output).
        if (!sample_block.has(col_node->result_name))
            continue;

        /// Extract query bounding box from the constant geometry argument.
        double xmin = 0;
        double ymin = 0;
        double xmax = 0;
        double ymax = 0;
        if (!tryExtractConstBbox(const_node, xmin, ymin, xmax, ymax))
            continue;

        SpatialFilter filter;
        filter.geometry_column_name = col_node->result_name;
        filter.query_xmin = xmin;
        filter.query_ymin = ymin;
        filter.query_xmax = xmax;
        filter.query_ymax = ymax;
        result.push_back(std::move(filter));
    }

    return result;
}

bool rowGroupFailsSpatialFilters(
    const parq::RowGroup & rg_meta,
    const std::vector<Reader::PrimitiveColumnInfo> & primitive_columns,
    const std::vector<SpatialFilter> & filters)
{
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

} // namespace DB::Parquet
