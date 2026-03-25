#include <Processors/Formats/Impl/Parquet/GeoFilter.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/WKB.h>
#include <Core/Block.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <IO/ReadBufferFromMemory.h>

#include <cmath>
#include <cstring>

namespace DB::Parquet
{

namespace
{


/// Recursively walk a constant column (ColumnConst / ColumnTuple / ColumnArray) at the
/// given row index and accumulate min/max x/y. Handles:
///   • ColumnConst   — unwrap and recurse at row 0
///   • ColumnTuple(ColumnFloat64, ColumnFloat64)  — a 2D point
///   • ColumnArray(…) — iterate elements
void accumulateBboxFromColumn(
    const DB::IColumn & col, size_t row,
    double & xmin, double & ymin,
    double & xmax, double & ymax,
    bool & found)
{
    if (const auto * const_col = typeid_cast<const DB::ColumnConst *>(&col))
    {
        accumulateBboxFromColumn(const_col->getDataColumn(), 0, xmin, ymin, xmax, ymax, found);
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
        double x = x_col->getData()[row];
        double y = y_col->getData()[row];
        if (!std::isfinite(x) || !std::isfinite(y))
            return;
        xmin = std::min(xmin, x);
        ymin = std::min(ymin, y);
        xmax = std::max(xmax, x);
        ymax = std::max(ymax, y);
        found = true;
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
            accumulateBboxFromColumn(array_col->getData(), i, xmin, ymin, xmax, ymax, found);
    }
}

/// Accumulate bounding box from a CH-native GeometricObject (CartesianPoint / LineString / Polygon / …).
struct BboxAccumulator
{
    double xmin = std::numeric_limits<double>::infinity();
    double ymin = std::numeric_limits<double>::infinity();
    double xmax = -std::numeric_limits<double>::infinity();
    double ymax = -std::numeric_limits<double>::infinity();
    bool found = false;

    void add(double x, double y)
    {
        if (!std::isfinite(x) || !std::isfinite(y)) return;
        xmin = std::min(xmin, x);
        ymin = std::min(ymin, y);
        xmax = std::max(xmax, x);
        ymax = std::max(ymax, y);
        found = true;
    }

    void add(const DB::CartesianPoint & p) { add(p.x(), p.y()); }

    template <typename Container>
    void addAll(const Container & pts) { for (const auto & p : pts) add(p); }
};

bool tryExtractWkbBbox(std::string_view wkb,
                       double & xmin, double & ymin,
                       double & xmax, double & ymax)
{
    DB::ReadBufferFromMemory buf(wkb);
    BboxAccumulator acc;
    try
    {
        auto geo = DB::parseWKBFormat(buf);
        std::visit([&](const auto & g)
        {
            using T = std::decay_t<decltype(g)>;
            if constexpr (std::is_same_v<T, DB::CartesianPoint>)
            {
                acc.add(g);
            }
            else if constexpr (std::is_same_v<T, DB::LineString<DB::CartesianPoint>>)
            {
                acc.addAll(g);
            }
            else if constexpr (std::is_same_v<T, DB::Polygon<DB::CartesianPoint>>)
            {
                acc.addAll(g.outer());
            }
            else if constexpr (std::is_same_v<T, DB::MultiLineString<DB::CartesianPoint>>)
            {
                for (const auto & ls : g) acc.addAll(ls);
            }
            else if constexpr (std::is_same_v<T, DB::MultiPolygon<DB::CartesianPoint>>)
            {
                for (const auto & poly : g) acc.addAll(poly.outer());
            }
        }, geo);
    }
    catch (...) { return false; }

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
    xmin = std::numeric_limits<double>::infinity();
    ymin = std::numeric_limits<double>::infinity();
    xmax = -std::numeric_limits<double>::infinity();
    ymax = -std::numeric_limits<double>::infinity();
    bool found = false;
    accumulateBboxFromColumn(*raw, 0, xmin, ymin, xmax, ymax, found);
    return found;
}

/// Decode a little-endian IEEE 754 double from Parquet statistics binary encoding.
bool readParquetDouble(const std::string & s, double & out)
{
    if (s.size() != sizeof(double))
        return false;
    std::memcpy(&out, s.data(), sizeof(double));
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

        double rg_xmin = 0;
        double rg_ymin = 0;
        double rg_xmax = 0;
        double rg_ymax = 0;
        bool have_bbox = false;

        /// Prefer geospatial_statistics.bbox from the geometry column itself.
        const auto & col_meta = rg_meta.columns.at(geo_col->column_idx).meta_data;
        if (col_meta.__isset.geospatial_statistics
            && col_meta.geospatial_statistics.__isset.bbox)
        {
            const auto & bbox = col_meta.geospatial_statistics.bbox;
            rg_xmin = bbox.xmin;
            rg_ymin = bbox.ymin;
            rg_xmax = bbox.xmax;
            rg_ymax = bbox.ymax;
            have_bbox = true;
        }

        /// Fall back to covering.bbox column statistics.
        /// BboxColumnIndices stores Parquet column indices directly (into rg_meta.columns).
        if (!have_bbox && geo_col->covering_bbox_indices.has_value())
        {
            const auto & idx = *geo_col->covering_bbox_indices;
            double v_xmin = 0;
            double v_ymin = 0;
            double v_xmax = 0;
            double v_ymax = 0;

            auto read_min = [&](size_t col_idx, double & out) -> bool
            {
                if (col_idx >= rg_meta.columns.size()) return false;
                const auto & cmeta = rg_meta.columns.at(col_idx).meta_data;
                return cmeta.__isset.statistics
                    && cmeta.statistics.__isset.min_value
                    && readParquetDouble(cmeta.statistics.min_value, out);
            };
            auto read_max = [&](size_t col_idx, double & out) -> bool
            {
                if (col_idx >= rg_meta.columns.size()) return false;
                const auto & cmeta = rg_meta.columns.at(col_idx).meta_data;
                return cmeta.__isset.statistics
                    && cmeta.statistics.__isset.max_value
                    && readParquetDouble(cmeta.statistics.max_value, out);
            };

            if (read_min(idx.xmin_col, v_xmin)
                && read_min(idx.ymin_col, v_ymin)
                && read_max(idx.xmax_col, v_xmax)
                && read_max(idx.ymax_col, v_ymax))
            {
                rg_xmin = v_xmin;
                rg_ymin = v_ymin;
                rg_xmax = v_xmax;
                rg_ymax = v_ymax;
                have_bbox = true;
            }
        }

        if (!have_bbox)
            continue;

        /// If the row group bbox is disjoint from the query bbox, no row in this
        /// group can satisfy the predicate → the whole row group can be skipped.
        bool disjoint = rg_xmax < filter.query_xmin
                     || rg_xmin > filter.query_xmax
                     || rg_ymax < filter.query_ymin
                     || rg_ymin > filter.query_ymax;

        if (disjoint)
            return true;
    }

    return false;
}

} // namespace DB::Parquet
