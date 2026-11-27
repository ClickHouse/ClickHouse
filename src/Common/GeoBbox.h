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
#include <Core/Field.h>
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
    bool valid = true; // false if any non-finite coordinate was seen; pruning must fail closed

    void add(double x, double y)
    {
        if (!std::isfinite(x) || !std::isfinite(y)) { valid = false; return; }
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
            collectSpatialFiltersConjunctive(*child, visited, try_extract_spatial_filter, result);
        return;
    }

    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return;

    if (node.function_base->getName() == "and")
    {
        for (const auto * child : node.children)
            collectSpatialFiltersConjunctive(*child, visited, try_extract_spatial_filter, result);
        return;
    }

    /// Non-`and` function: attempt extraction, do not recurse further.
    if (auto filter = try_extract_spatial_filter(node))
        result.push_back(std::move(*filter));
}

namespace GeoBboxDetail
{

inline std::optional<double> fieldToDouble(const Field & f)
{
    switch (f.getType())
    {
        case Field::Types::Float64: return f.safeGet<Float64>();
        case Field::Types::Int64:   return static_cast<double>(f.safeGet<Int64>());
        case Field::Types::UInt64:  return static_cast<double>(f.safeGet<UInt64>());
        default: return std::nullopt;
    }
}

/// A ring is an Array whose every element is a Tuple(x, y) — the native
/// `pointInPolygon`/`polygonsIntersect`/`polygonsWithin` literal syntax `[(x1, y1), ...]`.
inline bool isRingArray(const Array & array)
{
    return !array.empty() && std::ranges::all_of(array, [](const Field & e) { return e.getType() == Field::Types::Tuple; });
}

/// A polygon (with holes) is an Array of rings: the first is the shell, the rest are holes.
inline bool isPolygonArray(const Array & array)
{
    return !array.empty() && std::ranges::all_of(array, [](const Field & e)
    {
        return e.getType() == Field::Types::Array && isRingArray(e.safeGet<Array>());
    });
}

/// A multipolygon is an Array of polygons.
inline bool isMultiPolygonArray(const Array & array)
{
    return !array.empty() && std::ranges::all_of(array, [](const Field & e)
    {
        return e.getType() == Field::Types::Array && isPolygonArray(e.safeGet<Array>());
    });
}

/// Append the points of `array` (a ring, per `isRingArray`) to `out_ring`. Returns false if any
/// element is not a well-formed 2D point.
inline bool appendRing(const Array & array, Polygon<CartesianPoint>::ring_type & out_ring)
{
    for (const auto & elem : array)
    {
        const auto & tuple = elem.safeGet<Tuple>();
        if (tuple.size() < 2)
            return false;
        auto x = fieldToDouble(tuple[0]);
        auto y = fieldToDouble(tuple[1]);
        if (!x.has_value() || !y.has_value())
            return false;
        out_ring.emplace_back(*x, *y);
    }
    return true;
}

/// Build a full Polygon (shell + holes) from `array` (per `isPolygonArray`). Returns false if any
/// ring is malformed.
inline bool buildPolygon(const Array & array, Polygon<CartesianPoint> & out_polygon)
{
    if (!appendRing(array[0].safeGet<Array>(), out_polygon.outer()))
        return false;
    for (size_t i = 1; i < array.size(); ++i)
    {
        out_polygon.inners().emplace_back();
        if (!appendRing(array[i].safeGet<Array>(), out_polygon.inners().back()))
            return false;
    }
    return true;
}

}

/// Extract bbox from a single Field value (not a column).
/// Handles Tuple (native geometry points), Array (nested geometry collections),
/// and String (WKB-encoded geometry).
static bool extractBboxFromFieldValue(const Field & field, BboxAccumulator & acc)
{
    using namespace GeoBboxDetail;
    const auto type = field.getType();

    /// Tuple with at least two numeric elements — a single point.
    /// SQL integer literals produce Int64/UInt64 fields, so we coerce all numeric
    /// field types to double rather than accepting only Float64.
    if (type == Field::Types::Tuple)
    {
        const auto & tuple = field.safeGet<Tuple>();
        if (tuple.size() >= 2)
        {
            auto x = fieldToDouble(tuple[0]);
            auto y = fieldToDouble(tuple[1]);
            if (x.has_value() && y.has_value())
                acc.add(*x, *y);
        }
        return acc.found;
    }

    /// Array — recurse into each element.
    if (type == Field::Types::Array)
    {
        const auto & array = field.safeGet<Array>();

        /// Ring / polygon (with holes) / multipolygon literals must be validated the same way
        /// `parseConstPolygon`/`parseConstMultiPolygon` validate them at execute time (assembled
        /// shell + holes, `bg::correct` + `bg::is_valid`), not ring-by-ring in isolation — a hole
        /// entirely outside its shell is invalid even though each ring is individually simple.
        if (isRingArray(array))
        {
            Polygon<CartesianPoint> polygon;
            if (!appendRing(array, polygon.outer()))
            {
                acc.valid = false;
                return false;
            }
            boost::geometry::correct(polygon);
            std::string failure_message;
            if (!boost::geometry::is_valid(polygon, failure_message))
            {
                acc.valid = false;
                return false;
            }
            acc.addAll(polygon.outer());
            return acc.found;
        }

        if (isPolygonArray(array))
        {
            Polygon<CartesianPoint> polygon;
            if (!buildPolygon(array, polygon))
            {
                acc.valid = false;
                return false;
            }
            boost::geometry::correct(polygon);
            std::string failure_message;
            if (!boost::geometry::is_valid(polygon, failure_message))
            {
                acc.valid = false;
                return false;
            }
            acc.addAll(polygon.outer());
            return acc.found;
        }

        if (isMultiPolygonArray(array))
        {
            MultiPolygon<CartesianPoint> multi_polygon;
            for (const auto & poly_elem : array)
            {
                multi_polygon.emplace_back();
                if (!buildPolygon(poly_elem.safeGet<Array>(), multi_polygon.back()))
                {
                    acc.valid = false;
                    return false;
                }
            }
            boost::geometry::correct(multi_polygon);
            std::string failure_message;
            if (!boost::geometry::is_valid(multi_polygon, failure_message))
            {
                acc.valid = false;
                return false;
            }
            for (const auto & poly : multi_polygon)
                acc.addAll(poly.outer());
            return acc.found;
        }

        for (const auto & elem : array)
            extractBboxFromFieldValue(elem, acc);
        return acc.found;
    }

    /// String — try WKB parsing.
    if (type == Field::Types::String)
    {
        const auto & sv = field.safeGet<String>();
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
                else if constexpr (std::is_same_v<T, MultiPoint<CartesianPoint>>)
                    acc.addAll(g);
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
        return acc.found;
    }

    return false;
}

/// Try to extract a bounding box from a constant column.
/// Handles: WKB-encoded String (via parseWKBFormat), CH native geometry
/// (Tuple(Float64,Float64) for points, nested Array(Tuple) for collections).
/// Returns true and sets xmin/ymin/xmax/ymax on success.
inline bool tryExtractBboxFromColumn(
    const IColumn & col,
    double & xmin, double & ymin,
    double & xmax, double & ymax)
{
    BboxAccumulator acc;

    /// Unwrap ColumnConst to get the underlying data column.
    const IColumn * data_col = &col;
    if (const auto * const_col = typeid_cast<const ColumnConst *>(&col))
        data_col = &const_col->getDataColumn();

    /// Single-element column: extract the field value at index 0.
    if (data_col->size() == 1)
    {
        Field field;
        data_col->get(0, field);
        if (!extractBboxFromFieldValue(field, acc) || !acc.valid)
            return false;
    }
    else
    {
        /// Multi-element column: iterate all rows.
        for (size_t i = 0; i < data_col->size(); ++i)
        {
            Field field;
            data_col->get(i, field);
            extractBboxFromFieldValue(field, acc);
            if (!acc.valid)
                return false;
        }
    }

    xmin = acc.xmin;
    ymin = acc.ymin;
    xmax = acc.xmax;
    ymax = acc.ymax;
    return acc.found;
}

}
