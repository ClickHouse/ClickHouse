#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <Common/WKB.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromMemory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// Accumulates a bounding box from (x, y) coordinate pairs.
/// Check `found` before using xmin/ymin/xmax/ymax.
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

    /// Iterate over a container whose elements have .x() and .y() methods (e.g. CartesianPoint).
    template <typename Container>
    void addAll(const Container & pts) { for (const auto & p : pts) add(p.x(), p.y()); }
};

/// Recursively collect spatial filters from `node` only when they are in a
/// conjunctive-only context (AND branches). Traverses `and` function nodes;
/// stops at `or` or any non-spatial leaf to preserve boolean semantics.
///
/// `try_extract_spatial_filter` is called for each non-`and` function node.
/// It should return an optional spatial filter on success (with the extracted
/// query bbox), or nullopt if the node cannot be used for pruning.
template <typename Result, typename Extractor>
void collectSpatialFiltersConjunctive(
    const ActionsDAG::Node & node,
    std::unordered_set<const ActionsDAG::Node *> & visited,
    Extractor try_extract_spatial_filter,
    Result & result)
{
    if (!visited.insert(&node).second)
        return;

    if (node.type == ActionsDAG::ActionType::ALIAS)
    {
        for (const auto * child : node.children)
            collectSpatialFiltersConjunctive(*child, visited, std::move(try_extract_spatial_filter), result);
        return;
    }

    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return;

    if (node.function_base->getName() == "and")
    {
        for (const auto * child : node.children)
            collectSpatialFiltersConjunctive(*child, visited, std::move(try_extract_spatial_filter), result);
        return;
    }

    /// Non-`and` function: attempt extraction, do not recurse further.
    if (auto filter = try_extract_spatial_filter(node))
        result.push_back(std::move(*filter));
}

/// Try to extract a bounding box from a constant column.
/// Handles: WKB-encoded String (via parseWKBFormat), CH native geometry
/// (ColumnTuple<Float64,Float64> for points, ColumnArray of tuples for collections).
/// Returns true and sets xmin/ymin/xmax/ymax on success.
inline bool tryExtractBboxFromColumn(
    const IColumn & col,
    double & xmin, double & ymin,
    double & xmax, double & ymax)
{
    BboxAccumulator acc;

    /// Case 1: WKB-encoded String (used by st_intersects, st_geomfromgeojson, etc.)
    if (const auto * str_col = typeid_cast<const ColumnString *>(&col))
    {
        if (!str_col->empty())
        {
            auto sv = str_col->getDataAt(0);
            ReadBufferFromMemory buf(sv.data(), sv.size());
            try
            {
                auto geo = parseWKBFormat(buf);
                std::visit([&]<typename T>(const T & g)
                {
                    if constexpr (std::is_same_v<T, CartesianPoint>)
                        acc.add(g.x(), g.y());
                    else if constexpr (std::is_same_v<T, LineString<CartesianPoint>>)
                        acc.addAll(g);
                    else if constexpr (std::is_same_v<T, Polygon<CartesianPoint>>)
                        acc.addAll(g.outer());
                    else if constexpr (std::is_same_v<T, MultiLineString<CartesianPoint>>)
                        for (const auto & ls : g)
                            acc.addAll(ls);
                    else if constexpr (std::is_same_v<T, MultiPolygon<CartesianPoint>>)
                        for (const auto & poly : g)
                            acc.addAll(poly.outer());
                    else
                        static_assert(!sizeof(T), "Unhandled geometry type — add a case here");
                }, geo);
            }
            catch (...)
            {
                LOG_TRACE(getLogger("GeoBbox"), "Failed to parse WKB geometry for bbox extraction: {}", getCurrentExceptionMessage(false));
                return false;
            }
        }
        else
        {
            return false;
        }
    }
    /// Case 2: Native geometry — Tuple(Float64, Float64) (single point)
    else if (const auto * tuple_col = typeid_cast<const ColumnTuple *>(&col))
    {
        if (tuple_col->tupleSize() >= 2)
        {
            const auto * x_col = typeid_cast<const ColumnFloat64 *>(&tuple_col->getColumn(0));
            const auto * y_col = typeid_cast<const ColumnFloat64 *>(&tuple_col->getColumn(1));
            if (x_col && y_col && !x_col->empty())
                acc.add(x_col->getData()[0], y_col->getData()[0]);
        }
    }
    /// Case 3: Array(Tuple(Float64,Float64)) — multi-point / polygon ring
    else if (const auto * arr_col = typeid_cast<const ColumnArray *>(&col))
    {
        const auto * arr_tuple_col = typeid_cast<const ColumnTuple *>(&arr_col->getData());
        if (arr_tuple_col && arr_tuple_col->tupleSize() >= 2)
        {
            const auto * xs = typeid_cast<const ColumnFloat64 *>(&arr_tuple_col->getColumn(0));
            const auto * ys = typeid_cast<const ColumnFloat64 *>(&arr_tuple_col->getColumn(1));
            if (xs && ys)
            {
                const auto & offsets = arr_col->getOffsets();
                const size_t end = offsets.size() > 0 ? offsets.back() : 0;
                for (size_t i = 0; i < end; ++i)
                    acc.add(xs->getData()[i], ys->getData()[i]);
            }
        }
    }
    else
    {
        return false;
    }

    if (!acc.found)
        return false;
    xmin = acc.xmin;
    ymin = acc.ymin;
    xmax = acc.xmax;
    ymax = acc.ymax;
    return true;
}

}
