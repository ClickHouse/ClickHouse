#pragma once

#include <algorithm>
#include <array>
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
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/IDataType.h>
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
/// element is not a well-formed 2D point. A vertex with more than two coordinates fails the
/// extraction rather than silently using its first two: `Ring` is `Array(Tuple(Float64, Float64))`,
/// so `callOnGeometryDataType` raises `BAD_ARGUMENTS` ("Unknown geometry type") for a wider vertex
/// type, and a bbox derived from it would let pruning discard every granule and hide that
/// exception.
inline bool appendRing(const Array & array, Polygon<CartesianPoint>::ring_type & out_ring)
{
    for (const auto & elem : array)
    {
        const auto & tuple = elem.safeGet<Tuple>();
        if (tuple.size() != 2)
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
/// `allow_point_tuple` says the argument this field came from resolves to a `Point`, so a lone
/// `Tuple(Float64, Float64)` may be read as one; see the `Tuple` branch below.
static bool extractBboxFromFieldValue(const Field & field, BboxAccumulator & acc, bool allow_point_tuple = false)
{
    using namespace GeoBboxDetail;
    const auto type = field.getType();

    /// A `Tuple` constant is opaque unless the caller confirmed the argument resolves to a `Point`.
    /// A `Field` cannot tell one apart from an auxiliary tuple with entirely different semantics --
    /// a WASM UDF's `Tuple(Float64, Float64, Float64, Float64)` bbox argument, say -- so the
    /// argument's type has to decide it. A confirmed point contributes a zero-area bbox at its
    /// coordinate: `pointInPolygon` can only be true for rows whose polygon bbox contains it.
    if (type == Field::Types::Tuple)
    {
        if (!allow_point_tuple)
            return false;
        const auto & tuple = field.safeGet<Tuple>();
        /// Exactly two coordinates, matching `pointInPolygon`'s own `validate_tuple`, which raises
        /// `BAD_ARGUMENTS` ("must have exactly two elements") for anything wider. A wider tuple
        /// reaches this code through a `Dynamic`/`Variant` constant, which passes argument
        /// type-checking during analysis, so a bbox derived from its first two coordinates would
        /// prune every granule and hide the exception the predicate raises once evaluated.
        if (tuple.size() != 2)
            return false;
        auto x = fieldToDouble(tuple[0]);
        auto y = fieldToDouble(tuple[1]);
        if (!x.has_value() || !y.has_value())
            return false;
        acc.add(*x, *y);
        return acc.found;
    }

    /// Array — recurse into each element.
    if (type == Field::Types::Array)
    {
        const auto & array = field.safeGet<Array>();

        /// An EMPTY piece poisons extraction -- but only where the predicate would really raise on
        /// it. `isRingArray` requires a non-empty array, so a `Polygon`/`MultiPolygon` carrying an
        /// empty ring or an empty polygon -- say `[shell, []]` -- misses the assembled-geometry
        /// branches below and falls into the generic recursion, which would keep the bbox of the
        /// non-empty pieces and silently skip the empty one. With validation on,
        /// `parseConstPolygon`/`parseConstMultiPolygon` assemble the very same literal and reject it
        /// with `bg::is_valid`, so the query must raise rather than prune every disjoint granule
        /// away. A top-level empty array -- `CAST([], 'Ring')` -- is the same value seen from one
        /// level up, and must be reported as failed rather than as no information, or a sibling
        /// conjunct can still hide the exception.
        if (array.empty())
        {
            acc.valid = false;
            return false;
        }

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

    /// String — try WKB parsing. A predicate that does not accept a WKB payload at this argument
    /// position rejects it from its own `getReturnTypeImpl`, during analysis, so the exception
    /// cannot be pruned away and a bbox derived here can only cost pruning, never correctness.
    if (type == Field::Types::String)
    {
        const auto & sv = field.safeGet<String>();
        ReadBufferFromMemory buf(sv.data(), sv.size());
        try
        {
            auto geo = parseWKBFormat(buf);
            bool geometry_valid = true;
            std::visit([&]<typename T>(const T & g)
            {
                if constexpr (std::is_same_v<T, CartesianPoint>)
                    acc.add(g.x(), g.y());
                else if constexpr (std::is_same_v<T, LineString<CartesianPoint>>)
                    acc.addAll(g);
                else if constexpr (std::is_same_v<T, Polygon<CartesianPoint>>)
                {
                    /// Validate the same way the Array-literal ring/polygon branches above do --
                    /// a self-intersecting or otherwise invalid WKB polygon is guaranteed to raise
                    /// on evaluation, so it must fail closed rather than silently contribute no bbox.
                    Polygon<CartesianPoint> polygon = g;
                    boost::geometry::correct(polygon);
                    std::string failure_message;
                    if (!boost::geometry::is_valid(polygon, failure_message))
                    {
                        geometry_valid = false;
                        return;
                    }
                    acc.addAll(polygon.outer());
                }
                else if constexpr (std::is_same_v<T, MultiPoint<CartesianPoint>>)
                    acc.addAll(g);
                else if constexpr (std::is_same_v<T, MultiLineString<CartesianPoint>>)
                    for (const auto & ls : g)
                        acc.addAll(ls);
                else if constexpr (std::is_same_v<T, MultiPolygon<CartesianPoint>>)
                {
                    MultiPolygon<CartesianPoint> multi_polygon = g;
                    boost::geometry::correct(multi_polygon);
                    std::string failure_message;
                    if (!boost::geometry::is_valid(multi_polygon, failure_message))
                    {
                        geometry_valid = false;
                        return;
                    }
                    for (const auto & poly : multi_polygon)
                        acc.addAll(poly.outer());
                }
                else
                    static_assert(!sizeof(T), "Unhandled geometry type — add a case here");
            }, geo);

            if (!geometry_valid)
            {
                acc.valid = false;
                return false;
            }
        }
        catch (...)
        {
            LOG_TRACE(getLogger("GeoBbox"), "Failed to parse WKB geometry for bbox extraction: {}", getCurrentExceptionMessage(false));
            acc.valid = false;
            return false;
        }
        return acc.found;
    }

    return false;
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

/// The geometry-domain type name a type is DECLARED to carry -- its own custom name (`Point`,
/// `Ring`, `LineString`, ...), or, for a `Variant` (`Geometry` is a `Variant` over the geometry
/// kinds) or a `Dynamic`, the custom name of the concrete alternative stored in it. Empty when
/// nothing further can be said, such as for a raw array literal with no custom name.
///
/// `ColumnVariant::get`/`ColumnDynamic::get` return only the active alternative's flattened value
/// and discard which alternative it was, so the name must be read from the type/discriminator here,
/// before flattening, rather than guessed from the `Field`'s shape.
inline std::string geoKindNameOfType(const IDataType & type)
{
    /// A declared geometry kind can sit under a `Nullable`/`LowCardinality` wrapper, which carries no
    /// custom name of its own -- reading the outer type alone reports no kind at all and fails OPEN.
    /// That matters most for a WKB `String`: `tryExtractConstGeoField` flattens the non-null value to
    /// a plain `String` `Field` and `extractBboxFromFieldValue` decodes a perfectly usable bbox from
    /// it, so every granule gets pruned. Evaluation would have rejected the nested kind --
    /// `useDefaultImplementationForNulls` strips the `Nullable` first -- but on a fully pruned granule
    /// it never runs: for nullable inputs on a `0`-row block `IFunction` returns an empty result
    /// without calling the nested function at all, so the answer is a silent `0` rather than the
    /// exception the query must surface. Unwrap first, so the kind under the wrapper is the one the
    /// predicate is asked about.
    const IDataType * inner = &type;
    while (true)
    {
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(inner))
            inner = low_cardinality_type->getDictionaryType().get();
        else if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(inner))
            inner = nullable_type->getNestedType().get();
        else
            break;
    }

    if (const auto * custom_name = inner->getCustomName())
        return custom_name->getName();
    if (WhichDataType(*inner).isString())
        return "String";
    return {};
}

/// Helpers backing the geometry-kind checks below; kept out of `DB` so their generic names cannot
/// collide with anything else declared at that scope.
namespace GeoBboxDetail
{

/// Unwrap the `Nullable`/`LowCardinality` wrappers that carry no kind of their own, so the type
/// underneath is the one the predicate is actually asked about.
inline const IDataType & unwrapGeoKindWrappers(const IDataType & type)
{
    const IDataType * inner = &type;
    while (true)
    {
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(inner))
            inner = low_cardinality_type->getDictionaryType().get();
        else if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(inner))
            inner = nullable_type->getNestedType().get();
        else
            break;
    }

    return *inner;
}

/// The geometry kind an UNNAMED type resolves to, structurally, exactly as `callOnGeometryDataType`
/// does: it compares the type with `IDataType::equals`, which ignores custom names, so a raw
/// `Tuple(Float64, Float64)` is read as a `Point`, `Array(Tuple(Float64, Float64))` as a `Ring`, and
/// one more `Array` level each as a `Polygon` and a `MultiPolygon`. `geoKindNameOfType` reports no
/// kind for those types -- they carry no custom name -- yet the overload built for them at execution
/// time is perfectly well determined, so the predicate can be asked about it after all. Without
/// this, every unnamed `Dynamic`/`Variant` alternative had to be treated as an unknown kind and fail
/// closed, which cost pruning for queries that are guaranteed not to raise, e.g.
/// `pointInPolygon(CAST((0., 0.), 'Tuple(Float64, Float64)')::Dynamic, indexed_polygon_column)`.
/// Returns an empty name for anything that is not one of those shapes (`Ring`/`LineString` and
/// `Polygon`/`MultiLineString` are told apart only by a custom name, which by construction is absent
/// here, and `callOnGeometryDataType` resolves the unnamed shapes to `Ring`/`Polygon`).
inline std::string_view structuralGeoKindName(const IDataType & type)
{
    const IDataType * inner = &unwrapGeoKindWrappers(type);
    if (inner->getCustomName())
        return {};

    /// `Point`, `Ring`, `Polygon`, `MultiPolygon` -- one `Array` level apart each.
    static constexpr std::array kind_names = {"Point", "Ring", "Polygon", "MultiPolygon"};
    size_t depth = 0;
    while (const auto * array_type = typeid_cast<const DataTypeArray *>(inner))
    {
        if (++depth >= kind_names.size())
            return {};
        inner = &unwrapGeoKindWrappers(*array_type->getNestedType());
    }

    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(inner);
    if (!tuple_type || tuple_type->hasExplicitNames() || tuple_type->getElements().size() != 2)
        return {};
    for (const auto & element : tuple_type->getElements())
        if (!WhichDataType(*element).isFloat64())
            return {};

    return kind_names[depth];
}

/// Whether `type` resolves its geometry kind only at execution time -- a `Dynamic`, or a `Variant`
/// (`Geometry` is a `Variant` over the geometry kinds).
///
/// Such an argument vetoes pruning for the whole conjunction, unconditionally. Which overload runs
/// -- and so whether it raises `ILLEGAL_TYPE_OF_ARGUMENT` -- is settled per row by
/// `ExecutableFunctionDynamicAdaptor`/`ExecutableFunctionVariantAdaptor`, and on a block a sibling
/// conjunct pruned to `0` rows those adaptors return an empty result without ever building the
/// raising overload: the query answers `0` instead of surfacing the exception.
///
/// Deliberately blunt. A finer rule -- inspect the `Variant`'s alternatives, or a constant's stored
/// discriminator, and ask the predicate about each resolved kind -- is what this used to do, and it
/// needed per-kind knowledge from every predicate to stay both correct and useful. Since no shipped
/// query has a `Dynamic`/`Variant` geometry in a spatial predicate today, the pruning given up here
/// is theoretical, while the machinery it removes was not. Refining it is
/// https://github.com/ClickHouse/ClickHouse/issues/117163.
inline bool isDeferredGeometryKindType(const IDataType & type)
{
    const IDataType & inner = unwrapGeoKindWrappers(type);
    return typeid_cast<const DataTypeVariant *>(&inner) || typeid_cast<const DataTypeDynamic *>(&inner);
}

/// Whether `type` carries no geometry `extractBboxFromFieldValue` could read: neither a named
/// geometry kind (a WKB `String` included, see `geoKindNameOfType`), nor one of the unnamed shapes
/// `callOnGeometryDataType` resolves structurally, nor a `Dynamic`/`Variant` whose kind is only
/// known per row.
///
/// Checked at EVERY `Array` level, not just the outermost. `extractBboxFromFieldValue` recurses into
/// a generic `Array` and reads whatever each element turns out to be, so `Array(String)` is a
/// perfectly good carrier of WKB blobs -- and a corrupt blob nested in one must still fail the
/// conjunct closed, exactly as a top-level corrupt blob does. Answering `true` for the outer `Array`
/// alone would skip the whole recursion and downgrade that to `NoInfo`, letting a sibling conjunct
/// prune the granule and hide the parse failure. Only a type that is no geometry carrier at any
/// depth -- `Array(Int32)`, say -- may be skipped.
inline bool isNonGeometryType(const IDataType & type)
{
    const IDataType * inner = &unwrapGeoKindWrappers(type);
    while (true)
    {
        if (typeid_cast<const DataTypeVariant *>(inner) || typeid_cast<const DataTypeDynamic *>(inner))
            return false;
        if (!geoKindNameOfType(*inner).empty() || !structuralGeoKindName(*inner).empty())
            return false;

        const auto * array_type = typeid_cast<const DataTypeArray *>(inner);
        if (!array_type)
            return true;
        inner = &unwrapGeoKindWrappers(*array_type->getNestedType());
    }
}

}

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
/// A single constant argument is validated as a self-contained shell+holes/`MultiPolygon` by
/// `extractBboxFromFieldValue`. Two or more constant GEOMETRY arguments never contribute a bbox:
/// how a predicate combines them is its own business (`pointInPolygon`'s shell+holes assembly,
/// a WASM UDF's entirely different semantics), and whether the combination is even valid can
/// only be decided by assembling it -- see the comment above `getReturnTypeImpl` in
/// `pointInPolygon.cpp`. Such a node yields `NoInfo`, so the query stays correct and is simply
/// not pruned.
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

    /// A predicate whose result type is `Nullable(Nothing)` never runs at all, so nothing it could
    /// raise is left for pruning to hide. That is the strongest lenient state of
    /// `variant_throw_on_type_mismatch = 0` / `dynamic_throw_on_type_mismatch = 0`: when EVERY
    /// alternative of the `Variant`/`Dynamic` argument is incompatible, `FunctionBaseVariantAdaptor`/
    /// `FunctionBaseDynamicAdaptor` resolve the whole node to `Nullable(Nothing)` and their
    /// `ExecutableFunction` returns NULL rows without ever building the wrapped spatial predicate --
    /// including for a rejection at a DIFFERENT argument position, which the kind checks below would
    /// otherwise fail closed for (e.g. a WKB `String` in `pointInPolygon`'s polygon argument, where
    /// `getReturnTypeImpl` raises on argument 0 before it ever inspects argument 1). Returning
    /// `NotApplicable` lets a sibling conjunct on an indexed column keep its pruning; the query is
    /// still correct, since this conjunct is NULL on every row and selects nothing anyway.
    if (node.result_type && typeid_cast<const DataTypeNothing *>(&GeoBboxDetail::unwrapGeoKindWrappers(*node.result_type)))
        return NodeBboxStatus::NotApplicable;

    /// Any argument besides the single accepted geometry column input and constant geometry
    /// literals (e.g. a second column, or a non-constant expression) means the predicate's
    /// truth can depend on something the bbox can't see.
    const ActionsDAG::Node * input_child = nullptr;
    bool has_extra_non_constant = false;
    bool any_extraction_failed = false;
    bool any_deferred_kind_rejection = false;
    /// A bare `Tuple(Float64, Float64)` counts as a point, not only a `Point`-named type:
    /// `callOnGeometryDataType` resolves the unnamed shape to a `Point` too, so the same overload
    /// runs. Safe only because the areal predicates reject an argument they refuse from
    /// `getReturnTypeImpl`, during analysis, where pruning cannot turn it into a silent `0`.
    struct ConstGeoField
    {
        bool is_point = false;
        Field field;
    };
    std::vector<ConstGeoField> const_fields;
    for (const auto * child : node.children)
    {
        if (child->type == ActionsDAG::ActionType::INPUT)
        {
            /// Checked for EVERY input, accepted or not: `accept_input` rejecting a `Dynamic`/
            /// `Variant` column is precisely the case that used to fall through to
            /// `has_extra_non_constant` and let a sibling conjunct prune the exception away.
            if (child->result_type && GeoBboxDetail::isDeferredGeometryKindType(*child->result_type))
            {
                any_deferred_kind_rejection = true;
                continue;
            }

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

        /// A constant whose DECLARED type is no geometry at all carries no bbox and must not be
        /// interpreted by the shape of its `Field` below: that shape cannot tell `CAST([], 'Ring')`
        /// -- an empty geometry the execute-time `parseConstPolygon` rejects, which must fail closed
        /// -- from `CAST([], 'Array(Int32)')`, an auxiliary argument an `is_spatial_predicate = 1`
        /// WASM UDF declares and `FunctionUserDefinedWasm::getReturnTypeImpl` accepts, on which
        /// execution never raises. Failing closed for the latter would cost pruning for a query that
        /// cannot raise. The declared type tells the two apart, so read it rather than the shape.
        ///
        /// Skipping cannot hide an exception in the other direction either: a NATIVE predicate handed
        /// a non-geometry argument raises from `callOnTwoGeometryDataTypes`, which dispatches on the
        /// argument TYPES and so raises even on a fully pruned, zero-row block.
        if (child->result_type && GeoBboxDetail::isNonGeometryType(*child->result_type))
            continue;

        Field field;
        if (!tryExtractConstGeoField(*child, field))
        {
            any_extraction_failed = true;
            continue;
        }

        /// A `Dynamic`/`Variant` constant is vetoed too, for the same reason as the column above.
        if (child->result_type && GeoBboxDetail::isDeferredGeometryKindType(*child->result_type))
        {
            any_deferred_kind_rejection = true;
            continue;
        }

        const_fields.push_back(
            {child->result_type
                 && (geoKindNameOfType(GeoBboxDetail::unwrapGeoKindWrappers(*child->result_type)) == "Point"
                     || GeoBboxDetail::structuralGeoKindName(*child->result_type) == "Point"),
             std::move(field)});
    }

    /// A constant geometry argument that fails to extract is guaranteed to raise on evaluation,
    /// regardless of
    /// whether an accepted-column input is present. This -- and every validation below -- must be
    /// checked unconditionally, before `!input_child` or `has_extra_non_constant` are allowed to
    /// downgrade the result to `NotApplicable`/`NoInfo`: with
    /// `short_circuit_function_evaluation = 'disable'` this node is still evaluated on every row
    /// even when it doesn't reference the accepted column (e.g. a sibling `and` conjunct on a
    /// *different* geometry column), so an invalid constant geometry here must still veto pruning
    /// for the whole conjunction -- otherwise pruning could proceed using only an unrelated,
    /// valid conjunct's bbox, silently hiding the exception this conjunct is guaranteed to raise.
    if (any_extraction_failed || any_deferred_kind_rejection)
        return NodeBboxStatus::Failed;

    if (const_fields.empty())
        return input_child ? NodeBboxStatus::NoInfo : NodeBboxStatus::NotApplicable;

    /// Two or more constant geometry arguments aren't assembled into one combined shape (see
    /// the comment above). But it's a common signature
    /// for a `isSpatialPredicate()` UDF to take exactly ONE constant geometry argument alongside
    /// auxiliary constant scalars, e.g. `within_distance(geom_column, const_poly, 100)` -- that
    /// isn't really a "combine several geometries" case at all, so reduce it to the same
    /// single-geometry-constant bbox trusted below whenever exactly one of the constant arguments
    /// turns out to be geometry-shaped and the rest are not. Each argument that IS geometry-shaped
    /// is still guaranteed to be evaluated as one, and an invalid one is still guaranteed to
    /// raise; fail closed for those regardless of how many geometry-shaped arguments there are.
    /// `extractBboxFromFieldValue`'s `false` return is overloaded between "not geometry-shaped"
    /// and "geometry-shaped but invalid", and only `acc.valid == false` (poisoned by every
    /// genuine parse/validation failure) means the latter -- this includes a `String` constant
    /// that fails to parse as WKB: since a `String` argument's *intent* (WKB payload vs. an
    /// unrelated flag) can't be told apart from its `Field` alone, an unparseable one is always
    /// treated as invalid geometry and fails closed, even where it was only ever meant as e.g. a
    /// mode flag. Avoid raw `String` constants for non-geometry auxiliary arguments of an
    /// `is_spatial_predicate` function to keep pruning effective (see `wasm_udf.mdx`).
    std::optional<BboxAccumulator> single_geometry_field;
    if (const_fields.size() > 1)
    {
        bool multiple_geometry_fields = false;
        for (const auto & entry : const_fields)
        {
            BboxAccumulator field_acc;
            bool extracted = extractBboxFromFieldValue(
                entry.field, field_acc, entry.is_point);
            if (!field_acc.valid)
                return NodeBboxStatus::Failed;
            if (extracted)
            {
                if (single_geometry_field)
                    multiple_geometry_fields = true;
                else
                    single_geometry_field = field_acc;
            }
        }
        if (!single_geometry_field || multiple_geometry_fields)
            return input_child ? NodeBboxStatus::NoInfo : NodeBboxStatus::NotApplicable;
    }

    /// A single constant geometry argument is self-contained (shell + holes, or a full
    /// MultiPolygon, all in one literal) and is already validated as such by
    /// `extractBboxFromFieldValue`.
    double xmin = 0;
    double ymin = 0;
    double xmax = 0;
    double ymax = 0;
    if (single_geometry_field)
    {
        xmin = single_geometry_field->xmin;
        ymin = single_geometry_field->ymin;
        xmax = single_geometry_field->xmax;
        ymax = single_geometry_field->ymax;
    }
    else
    {
        /// Exactly one constant argument: the loop above returns for every larger `const_fields`,
        /// and `const_fields.empty()` was rejected earlier.
        chassert(const_fields.size() == 1);

        BboxAccumulator acc;
        bool extracted = extractBboxFromFieldValue(
            const_fields[0].field, acc, const_fields[0].is_point);
        if (!acc.valid)
            return NodeBboxStatus::Failed;
        if (!extracted)
        {
            /// Not recognized as a geometry shape at all (e.g. a plain scalar argument to a
            /// WASM UDF) -- no bbox can be derived from it, but it isn't guaranteed to raise
            /// either, so it must not force the whole conjunction closed. See the comment above
            /// the multi-constant-argument loop for why `extracted == false` alone isn't enough.
            return input_child ? NodeBboxStatus::NoInfo : NodeBboxStatus::NotApplicable;
        }
        xmin = acc.xmin;
        ymin = acc.ymin;
        xmax = acc.xmax;
        ymax = acc.ymax;
    }

    /// Only now -- after every constant geometry argument is confirmed extractable and valid --
    /// can the absence of an accepted-column input, or the presence of an extra non-constant
    /// argument, downgrade the result to `NotApplicable`/`NoInfo`: this node's truth may not be
    /// derivable as a bbox on the accepted column, but there's also no invalid geometry to fail
    /// closed for.
    if (!input_child)
        return NodeBboxStatus::NotApplicable;

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
