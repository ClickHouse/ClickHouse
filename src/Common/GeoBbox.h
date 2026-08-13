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

    /// Tuple with exactly two numeric elements — a single point, matching the `Point`
    /// domain type's own (Float64, Float64) contract. A wider tuple is not a point: it's
    /// opaque to bbox extraction (e.g. a `isSpatialPredicate()` WASM UDF could take a
    /// constant `Tuple(Float64, Float64, Float64, Float64)` bbox/rect argument with entirely
    /// different semantics). Silently reading just its first two elements as (x, y) would
    /// derive a bogus, too-small bbox and wrongly prune granules the predicate can still
    /// match on -- fail the extraction instead so pruning is disabled for it.
    /// SQL integer literals produce Int64/UInt64 fields, so we coerce all numeric
    /// field types to double rather than accepting only Float64.
    if (type == Field::Types::Tuple)
    {
        const auto & tuple = field.safeGet<Tuple>();
        if (tuple.size() == 2)
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

/// Multi-argument `pointInPolygon(point, arg1, arg2, ...)` combines its constant geometry
/// arguments differently depending on the shape of the first one, exactly mirroring
/// `FunctionPointInPolygon::executeImpl`'s `is_const_multi_polygon` decision:
///  - first argument a Polygon (an array of rings, per `isPolygonArray`): every argument is
///    a separate component of one `MultiPolygon` (`parseConstMultiPolygonFromMultipleColumns`).
///  - otherwise (first argument a flat Ring, per `isRingArray`): the first argument is the
///    shell and the rest are holes of one `Polygon` (`parseConstPolygonWithHolesFromMultipleColumns`).
/// Validates the ASSEMBLED geometry with `bg::is_valid`, not each argument in isolation --
/// a hole entirely outside its shell, or overlapping components in a multipolygon, are only
/// visible once the pieces are combined, and must fail closed the same way a single-argument
/// invalid polygon does.
inline bool tryExtractBboxFromMultiArgConstGeometry(
    const std::vector<const Field *> & args,
    double & xmin, double & ymin, double & xmax, double & ymax)
{
    using namespace GeoBboxDetail;

    if (args.size() < 2)
        return false;

    for (const auto * f : args)
        if (f->getType() != Field::Types::Array)
            return false;

    BboxAccumulator acc;
    std::string failure_message;

    if (isPolygonArray(args[0]->safeGet<Array>()))
    {
        MultiPolygon<CartesianPoint> multi_polygon;
        for (const auto * f : args)
        {
            const auto & array = f->safeGet<Array>();
            multi_polygon.emplace_back();
            if (isPolygonArray(array))
            {
                if (!buildPolygon(array, multi_polygon.back()))
                    return false;
            }
            else if (isRingArray(array))
            {
                if (!appendRing(array, multi_polygon.back().outer()))
                    return false;
            }
            else
                return false;
        }

        boost::geometry::correct(multi_polygon);
        if (!boost::geometry::is_valid(multi_polygon, failure_message))
            return false;

        for (const auto & poly : multi_polygon)
            acc.addAll(poly.outer());
    }
    else
    {
        Polygon<CartesianPoint> polygon;
        for (size_t i = 0; i < args.size(); ++i)
        {
            const auto & array = args[i]->safeGet<Array>();
            if (!isRingArray(array))
                return false;

            if (i == 0)
            {
                if (!appendRing(array, polygon.outer()))
                    return false;
            }
            else
            {
                polygon.inners().emplace_back();
                if (!appendRing(array, polygon.inners().back()))
                    return false;
            }
        }

        boost::geometry::correct(polygon);
        if (!boost::geometry::is_valid(polygon, failure_message))
            return false;

        acc.addAll(polygon.outer());
    }

    if (!acc.found)
        return false;

    xmin = acc.xmin;
    ymin = acc.ymin;
    xmax = acc.xmax;
    ymax = acc.ymax;
    return true;
}

/// Bounding box of a constant query geometry, shared by every pruning surface
/// (the `MergeTree` `spatial_bbox` skip index, `Parquet` row-group pruning, ...).
struct QueryBbox
{
    double xmin = 0, ymin = 0, xmax = 0, ymax = 0;
};

/// Outcome of trying to derive a `QueryBbox` from a single (non-`and`) spatial predicate node.
enum class NodeBboxStatus
{
    /// The node isn't a spatial predicate on an accepted geometry column at all (e.g. an
    /// unrelated function, or a spatial predicate on a column `accept_input` rejects) --
    /// carries no pruning information and cannot force a surrounding conjunction to fail closed.
    NotApplicable,
    /// The node is a spatial predicate on an accepted column, but no safe bbox could be
    /// derived from it (e.g. an extra non-constant argument, or more than one constant
    /// geometry argument on a non-`pointInPolygon` predicate) -- and there's no reason to
    /// think evaluating it would throw, so a surrounding conjunction may still prune using
    /// other conjuncts' bboxes.
    NoInfo,
    /// The node is a spatial predicate on an accepted column whose constant geometry
    /// argument(s) failed to validate (e.g. an invalid polygon) -- evaluating it is expected
    /// to raise an exception, so pruning based on any other conjunct's bbox could silently
    /// hide that exception.
    Failed,
    /// A bbox was successfully derived.
    Ok,
};

/// Extract the (single) `Field` value of a constant `ActionsDAG` `COLUMN` node, for combining
/// several such children into one assembled geometry (see `tryExtractBboxFromMultiArgConstGeometry`).
inline bool tryExtractConstGeoField(const ActionsDAG::Node & node, Field & out_field)
{
    if (!node.column || !node.is_deterministic_constant)
        return false;

    const IColumn * data_col = node.column.get();
    if (const auto * const_col = typeid_cast<const ColumnConst *>(data_col))
        data_col = &const_col->getDataColumn();

    if (data_col->size() != 1)
        return false;

    data_col->get(0, out_field);
    return true;
}

/// Derive a `QueryBbox` from a single spatial-predicate `ActionsDAG` node (not itself an
/// `and`), shared by every pruning surface. `accept_input(child)` decides whether an
/// `INPUT`-typed child counts as the one accepted geometry column input; on `Ok`,
/// `out_col_node` is set to that node so the caller can read its `result_name`.
///
/// Mirrors `FunctionPointInPolygon::executeImpl`'s handling of its constant geometry
/// argument(s): a single constant argument is validated as a self-contained shell+holes/
/// `MultiPolygon` by `extractBboxFromFieldValue`; two or more constant arguments are only
/// assembled together (via `tryExtractBboxFromMultiArgConstGeometry`) for `pointInPolygon`
/// itself -- any other `isSpatialPredicate()` function (e.g. a WASM UDF) with more than one
/// constant geometry argument could combine them under entirely different semantics, so
/// assuming `pointInPolygon`'s shell/hole assembly for it would produce a bogus bbox.
template <typename AcceptInput>
NodeBboxStatus extractSpatialPredicateNodeBbox(
    const ActionsDAG::Node & node,
    AcceptInput && accept_input,
    QueryBbox & out_bbox,
    const ActionsDAG::Node *& out_col_node)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base
        || !node.function_base->isSpatialPredicate() || node.children.size() < 2)
        return NodeBboxStatus::NotApplicable;

    /// Any argument besides the single accepted geometry column input and constant geometry
    /// literals (e.g. a second column, or a non-constant expression) means the predicate's
    /// truth can depend on something the bbox can't see.
    const ActionsDAG::Node * input_child = nullptr;
    bool has_extra_non_constant = false;
    bool any_extraction_failed = false;
    std::vector<Field> const_fields;

    for (const auto * child : node.children)
    {
        if (child->type == ActionsDAG::ActionType::INPUT)
        {
            if (!input_child && accept_input(*child))
            {
                input_child = child;
                continue;
            }
            has_extra_non_constant = true;
            continue;
        }

        if (child->type != ActionsDAG::ActionType::COLUMN || !child->is_deterministic_constant)
        {
            has_extra_non_constant = true;
            continue;
        }

        Field field;
        if (!tryExtractConstGeoField(*child, field))
        {
            any_extraction_failed = true;
            continue;
        }
        const_fields.push_back(std::move(field));
    }

    /// This predicate doesn't reference an accepted geometry column at all -- it carries no
    /// pruning information here. But with `short_circuit_function_evaluation = 'disable'` it
    /// is still evaluated on every row, so if one of its constant geometry arguments failed to
    /// extract (and is therefore guaranteed to raise on evaluation), that must still veto
    /// pruning -- otherwise pruning could proceed using only an unrelated, valid conjunct's
    /// bbox, silently hiding the exception this conjunct is guaranteed to raise.
    if (!input_child)
        return any_extraction_failed ? NodeBboxStatus::Failed : NodeBboxStatus::NotApplicable;

    /// A constant geometry argument that fails to extract or validate is guaranteed to raise
    /// on evaluation, regardless of whether other, non-constant arguments (e.g. an extra
    /// column) are also present. This must be checked -- and, on failure, veto pruning via
    /// `Failed` -- before `has_extra_non_constant` is allowed to downgrade the result to
    /// `NoInfo`; otherwise an extra non-constant argument would mask an invalid constant one.
    if (any_extraction_failed)
        return NodeBboxStatus::Failed;

    if (const_fields.empty())
        return NodeBboxStatus::NoInfo;

    if (const_fields.size() > 1 && node.function_base->getName() != "pointInPolygon")
        return NodeBboxStatus::NoInfo;

    /// A single constant geometry argument is self-contained (shell + holes, or a full
    /// MultiPolygon, all in one literal) and is already validated as such by
    /// extractBboxFromFieldValue. Two or more constant arguments (only reachable for
    /// `pointInPolygon` past the check above) must be assembled and validated together --
    /// validating each argument in isolation would miss e.g. a hole entirely outside its shell.
    double xmin = 0;
    double ymin = 0;
    double xmax = 0;
    double ymax = 0;
    bool ok = false;
    if (const_fields.size() == 1)
    {
        BboxAccumulator acc;
        ok = extractBboxFromFieldValue(const_fields[0], acc) && acc.valid;
        if (ok)
        {
            xmin = acc.xmin;
            ymin = acc.ymin;
            xmax = acc.xmax;
            ymax = acc.ymax;
        }
    }
    else
    {
        std::vector<const Field *> field_ptrs;
        field_ptrs.reserve(const_fields.size());
        for (const auto & f : const_fields)
            field_ptrs.push_back(&f);
        ok = tryExtractBboxFromMultiArgConstGeometry(field_ptrs, xmin, ymin, xmax, ymax);
    }

    if (!ok)
        return NodeBboxStatus::Failed;

    /// Only now -- after the constant geometry argument(s) are confirmed extractable and valid
    /// -- can an extra non-constant argument downgrade the result to "no info": the predicate's
    /// truth may depend on that argument, so its bbox can't be used to prune, but there's also
    /// no invalid geometry to fail closed for.
    if (has_extra_non_constant)
        return NodeBboxStatus::NoInfo;

    out_bbox = {xmin, ymin, xmax, ymax};
    out_col_node = input_child;
    return NodeBboxStatus::Ok;
}

/// Walk `node`, recursing only through `and` children (and transparently through `ALIAS`
/// nodes): an `or` (or any other function) can be true via a branch unrelated to an accepted
/// geometry column, so deriving a bbox from just one of its arguments would incorrectly prune.
/// Recurses through every `and` conjunct rather than stopping at the first that yields a bbox,
/// because with `short_circuit_function_evaluation = 'disable'` every conjunct is always
/// evaluated: pruning using only an earlier, valid conjunct's bbox would silently skip a later
/// conjunct whose invalid constant geometry is expected to raise an exception.
///
/// Calls `on_bbox(col_node, bbox)` for every conjunct that yields one. Sets `failed = true`
/// (and stops) as soon as any reachable conjunct is `NodeBboxStatus::Failed`, since pruning
/// must be disabled for the WHOLE conjunction in that case, not just for the failing conjunct.
template <typename AcceptInput, typename OnBbox>
void collectConjunctiveSpatialBboxes(
    const ActionsDAG::Node * node,
    std::unordered_set<const ActionsDAG::Node *> & visited,
    AcceptInput && accept_input,
    OnBbox && on_bbox,
    bool & failed)
{
    if (!node || failed || !visited.insert(node).second)
        return;

    if (node->type == ActionsDAG::ActionType::ALIAS
        || (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
            && node->function_base->getName() == "and"))
    {
        for (const auto * child : node->children)
        {
            collectConjunctiveSpatialBboxes(child, visited, accept_input, on_bbox, failed);
            if (failed)
                return;
        }
        return;
    }

    QueryBbox node_bbox;
    const ActionsDAG::Node * col_node = nullptr;
    switch (extractSpatialPredicateNodeBbox(*node, accept_input, node_bbox, col_node))
    {
        case NodeBboxStatus::Failed:
            failed = true;
            return;
        case NodeBboxStatus::Ok:
            on_bbox(*col_node, node_bbox);
            return;
        case NodeBboxStatus::NoInfo:
        case NodeBboxStatus::NotApplicable:
            return;
    }
}

}
