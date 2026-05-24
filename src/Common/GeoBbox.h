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

/// Extract bbox from a single Field value (not a column).
/// Handles Tuple (native geometry points), Array (nested geometry collections),
/// and String (WKB-encoded geometry).
static bool extractBboxFromFieldValue(const Field & field, BboxAccumulator & acc)
{
    const auto type = field.getType();

    /// Tuple with at least two numeric elements — a single point.
    /// SQL integer literals produce Int64/UInt64 fields, so we coerce all numeric
    /// field types to double rather than accepting only Float64.
    if (type == Field::Types::Tuple)
    {
        const auto & tuple = field.safeGet<Tuple>();
        if (tuple.size() >= 2)
        {
            auto fieldToDouble = [](const Field & f) -> std::optional<double>
            {
                switch (f.getType())
                {
                    case Field::Types::Float64: return f.safeGet<Float64>();
                    case Field::Types::Int64:   return static_cast<double>(f.safeGet<Int64>());
                    case Field::Types::UInt64:  return static_cast<double>(f.safeGet<UInt64>());
                    default: return std::nullopt;
                }
            };
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
        if (!extractBboxFromFieldValue(field, acc))
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
        }
    }

    xmin = acc.xmin;
    ymin = acc.ymin;
    xmax = acc.xmax;
    ymax = acc.ymax;
    return true;
}

}
