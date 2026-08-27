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
/// `require_valid` mirrors the predicate's own `IFunctionBase::requiresValidConstGeometry()`:
/// when false, the predicate's own evaluation never runs a topology check on this argument either
/// (e.g. `pointInPolygon` with `validate_polygons = 0`), so an invalid ring/polygon/multipolygon
/// must still contribute a bbox from its raw coordinates rather than failing closed -- only
/// structural parse failures (a ring that can't even be assembled, a WKB payload that doesn't
/// parse) remain unconditional, since those aren't validity checks and would fail at evaluation
/// time regardless of `require_valid`.
/// `allow_point_tuple` mirrors `IFunctionBase::treatsConstTupleAsPoint(arg_index)` for the
/// specific argument this field came from: true only for `pointInPolygon`'s own first (point)
/// argument, see the `Tuple` branch below.
static bool extractBboxFromFieldValue(const Field & field, BboxAccumulator & acc, bool require_valid = true, bool allow_point_tuple = false)
{
    using namespace GeoBboxDetail;
    const auto type = field.getType();

    /// A `Tuple` constant is opaque to bbox extraction by default, deliberately including the
    /// two-element case that would otherwise look like a `Point` domain type's own
    /// (Float64, Float64) contract: of the three builtins that currently set
    /// `isSpatialPredicate()` (`pointInPolygon`, `polygonsIntersectCartesian`,
    /// `polygonsWithinCartesian`), only `pointInPolygon`'s own first argument accepts a bare
    /// `Point` -- every other argument of all three unconditionally rejects one with
    /// `ILLEGAL_TYPE_OF_ARGUMENT` at evaluation time, including when it arrives via a
    /// `Geometry`/`Variant`-typed constant (e.g. `polygonsIntersectCartesian(poly,
    /// readWKT('POINT(0 0)'))`) rather than a raw literal, since a raw literal would already be
    /// caught by argument type-checking before ever reaching this code. Trusting a bbox derived
    /// from such a point would let pruning discard every granule and silently hide the exception
    /// the predicate is guaranteed to raise once evaluated on real data -- so, like a wider tuple
    /// (e.g. a `isSpatialPredicate()` WASM UDF's constant `Tuple(Float64, Float64, Float64,
    /// Float64)` bbox/rect argument with entirely different semantics), fail the extraction
    /// instead so pruning is disabled for it, UNLESS the caller has confirmed (via
    /// `allow_point_tuple`, derived from `treatsConstTupleAsPoint`) that this specific argument
    /// position of this specific predicate genuinely treats a lone point as meaningful: then it
    /// contributes a zero-area bbox at that single coordinate, since `pointInPolygon` can only be
    /// true for rows whose polygon bbox contains that exact point.
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

        /// An EMPTY piece poisons extraction. `isRingArray` requires a non-empty array, so a
        /// `Polygon`/`MultiPolygon` carrying an empty ring or an empty polygon -- say
        /// `[shell, []]` -- misses the assembled-geometry branches below and falls into the generic
        /// recursion, which would keep the bbox of the non-empty pieces and silently skip the empty
        /// one. `parseConstPolygon`/`parseConstMultiPolygon` assemble the very same literal and
        /// reject it with `bg::is_valid`, so the query must raise rather than prune every disjoint
        /// granule away. A top-level empty array -- `CAST([], 'Ring')` -- is the same value seen
        /// from one level up, and must be reported as failed rather than as no information, or a
        /// sibling conjunct can still hide the exception.
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
            if (require_valid)
            {
                std::string failure_message;
                if (!boost::geometry::is_valid(polygon, failure_message))
                {
                    acc.valid = false;
                    return false;
                }
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
            if (require_valid)
            {
                std::string failure_message;
                if (!boost::geometry::is_valid(polygon, failure_message))
                {
                    acc.valid = false;
                    return false;
                }
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
            if (require_valid)
            {
                std::string failure_message;
                if (!boost::geometry::is_valid(multi_polygon, failure_message))
                {
                    acc.valid = false;
                    return false;
                }
            }
            for (const auto & poly : multi_polygon)
                acc.addAll(poly.outer());
            return acc.found;
        }

        for (const auto & elem : array)
            extractBboxFromFieldValue(elem, acc, require_valid);
        return acc.found;
    }

    /// String — try WKB parsing. Whether the predicate accepts a WKB payload at this argument
    /// position at all is decided before this, by `extractSpatialPredicateNodeBbox`'s kind check
    /// (`constGeoKindName` reports a `String` under the kind name `String`) -- none of the
    /// builtins accepts one, so only a predicate that does, such as an `isSpatialPredicate()`
    /// WASM UDF reading raw WKB, ever gets here with a `String`.
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
                    /// on evaluation, so it must fail closed rather than silently contribute no bbox
                    /// -- unless require_valid is false, mirroring those branches too.
                    Polygon<CartesianPoint> polygon = g;
                    boost::geometry::correct(polygon);
                    if (require_valid)
                    {
                        std::string failure_message;
                        if (!boost::geometry::is_valid(polygon, failure_message))
                        {
                            geometry_valid = false;
                            return;
                        }
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
                    if (require_valid)
                    {
                        std::string failure_message;
                        if (!boost::geometry::is_valid(multi_polygon, failure_message))
                        {
                            geometry_valid = false;
                            return;
                        }
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

/// Get the geometry-domain type name of a constant `ActionsDAG` node, needed to tell whether a
/// constant is explicitly typed as a kind `IFunctionBase::rejectsConstGeometryKind` says this
/// predicate is guaranteed to reject -- either the node's own
/// custom name (a constant explicitly typed e.g. `LineString`), or, if the node's type is a
/// `Variant` (e.g. `Geometry`, which is a `Variant` over `Point`/`Ring`/`Polygon`/`LineString`/
/// `MultiPoint`/`MultiLineString`/`MultiPolygon` with a custom name of its own) or `Dynamic`, the
/// custom name of the concrete alternative/row type actually stored in the constant's single
/// row: `ColumnVariant::get`/`ColumnDynamic::get` (used by `tryExtractConstGeoField` above)
/// return only the active alternative's/row's own flattened value, discarding which
/// alternative/type it was, so that distinguishing name must be read from the type/discriminator
/// here, before flattening, rather than guessed from the `Field`'s shape. Returns an empty name
/// when nothing further can be said (e.g. a raw array literal, which has no custom name and is
/// interpreted the same, shape-only way by the predicate itself).
///
/// A plain `String` (a WKB payload, the geometry encoding every `Parquet`/`Iceberg` geo column
/// uses on disk) is reported under the kind name `String`: it has no custom name of its own, yet
/// it is just as much a declared geometry encoding as `Point` or `Polygon`, and a predicate knows
/// perfectly well whether it accepts one -- none of the builtins does (`pointInPolygon` raises
/// `ILLEGAL_TYPE_OF_ARGUMENT`, `polygonsIntersectCartesian`/`polygonsWithinCartesian` raise
/// `BAD_ARGUMENTS` from `callOnGeometryDataType`'s dispatch), while an `isSpatialPredicate()` WASM
/// UDF reading raw WKB does, and keeps its default `rejectsConstGeometryKind` of false. Without
/// this, the WKB branch of `extractBboxFromFieldValue` derived a bbox from a payload the predicate
/// is guaranteed to reject at execution time; see the `String` kind's own note there.
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

/// Helpers backing `hasDeferredGeometryKindRejection` below; kept out of `DB` so their generic
/// names cannot collide with anything else declared at that scope.
namespace GeoBboxDetail
{

/// Whether the geometry kind stored in `type` is only decided at execution time, and the predicate
/// may well reject it there. A `Dynamic` column, or a `Variant` (`Geometry` is a `Variant` over the
/// geometry kinds), carries no single declared kind an `ActionsDAG` node could be asked about: which
/// overload runs -- and therefore whether it raises `ILLEGAL_TYPE_OF_ARGUMENT` -- is settled per row.
/// Such an argument must fail closed for the whole conjunction. It is guaranteed to be evaluated on
/// every surviving row under `short_circuit_function_evaluation = 'disable'`, so it raises as soon as
/// a rejected kind is actually seen -- but if a sibling conjunct on the indexed column pruned every
/// granule first, `ExecutableFunctionDynamicAdaptor`/`ExecutableFunctionVariantAdaptor` return an
/// empty result without ever building the raising overload, and the query answers `0` instead of
/// surfacing the exception. That is the very exception hiding the constant-side checks above avoid,
/// only reached across columns.
///
/// A `Variant` names its alternatives, so it fails closed only when at least one of them is a kind
/// this predicate rejects at this position -- a `Variant` all of whose alternatives are accepted can
/// never raise, and vetoing pruning for it would cost pruning for nothing. A `Dynamic` can hold any
/// type at all, so it always fails closed for a predicate that rejects any kind whatsoever.
///
/// "Named" is the operative word: an alternative that carries no custom name of its own -- say a raw
/// `Tuple(Float64, Float64)` -- is still resolved to a concrete geometry by `callOnGeometryDataType`,
/// which compares the type STRUCTURALLY and reads that tuple as a `Point`. There is no kind name to
/// ask `rejectsColumnGeometryKind` about, yet the overload built per row can reject it just the
/// same, so an unnamed alternative must be treated as an unknown kind and fail closed exactly like a
/// `Dynamic`, not waved through as "says nothing".
inline bool rejectsAnyGeometryKind(const IFunctionBase & function, size_t arg_index)
{
    /// Probing the geometry kinds tells a predicate that rejects some kind apart from one that
    /// accepts everything and can never raise on kind grounds -- e.g. a WASM UDF reading raw WKB,
    /// which keeps its default `rejectsConstGeometryKind` of false and must keep pruning.
    static constexpr std::array kind_names
        = {"Point", "Ring", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "String"};
    return std::ranges::any_of(
        kind_names, [&](std::string_view kind_name) { return function.rejectsColumnGeometryKind(kind_name, arg_index); });
}

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

/// Whether the predicate rejects `type` at `arg_index` when `type` carries no kind name of its own:
/// by the structural kind it resolves to, if it resolves to one at all, and only otherwise by the
/// blanket "rejects any kind" test.
inline bool rejectsUnnamedGeometryType(const IDataType & type, const IFunctionBase & function, size_t arg_index)
{
    const auto structural_kind_name = structuralGeoKindName(type);
    if (!structural_kind_name.empty())
        return function.rejectsColumnGeometryKind(structural_kind_name, arg_index);
    return rejectsAnyGeometryKind(function, arg_index);
}

/// The geometry kind name a DECLARED argument type stands for, whether it is spelled with a custom
/// name (`Point`, `Ring`, `LineString`, ...) or structurally (`Tuple(Float64, Float64)`,
/// `Array(Tuple(Float64, Float64))`, ...). Empty when the type is no geometry at all.
///
/// This is the mirror image of the two lookups above: `geoKindNameOfType` answers for a named type
/// and `structuralGeoKindName` for an unnamed one, and a declared argument may be written either
/// way. It lets a predicate whose accepted kinds ARE its declared argument types -- an
/// `is_spatial_predicate` `LANGUAGE WASM` UDF, whose `getReturnTypeImpl` raises
/// `ILLEGAL_TYPE_OF_ARGUMENT` for anything else -- answer `rejectsColumnGeometryKind` by comparison
/// instead of leaving it at its default `false`.
inline std::string declaredGeoKindName(const IDataType & type)
{
    auto kind_name = geoKindNameOfType(type);
    if (!kind_name.empty())
        return kind_name;
    return std::string{structuralGeoKindName(type)};
}

/// Whether `type` resolves its geometry kind only at execution time -- a `Dynamic`, or a `Variant`
/// (`Geometry` is a `Variant` over the geometry kinds).
inline bool isDeferredGeometryKindType(const IDataType & type)
{
    const IDataType & inner = unwrapGeoKindWrappers(type);
    return typeid_cast<const DataTypeVariant *>(&inner) || typeid_cast<const DataTypeDynamic *>(&inner);
}

}

inline bool hasDeferredGeometryKindRejection(const IDataType & type, const IFunctionBase & function, size_t arg_index)
{
    const IDataType & inner = GeoBboxDetail::unwrapGeoKindWrappers(type);

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(&inner))
    {
        for (const auto & alternative : variant_type->getVariants())
        {
            const auto kind_name = geoKindNameOfType(*alternative);
            /// An unnamed alternative is an unknown kind, not an absent one: see the note above.
            /// It is unknown only where it does not resolve structurally, though -- see
            /// `structuralGeoKindName`.
            if (kind_name.empty() ? GeoBboxDetail::rejectsUnnamedGeometryType(*alternative, function, arg_index)
                                  : function.rejectsColumnGeometryKind(kind_name, arg_index))
                return true;
        }
        return false;
    }

    if (typeid_cast<const DataTypeDynamic *>(&inner))
    {
        /// Every kind a predicate could possibly reject is reachable here.
        return GeoBboxDetail::rejectsAnyGeometryKind(function, arg_index);
    }

    return false;
}

/// The concrete type actually stored in a constant `Dynamic`/`Variant` node's single row -- the
/// alternative whose overload will be built at execution time. Returns `nullptr` when the node is
/// not such a constant, or when the stored alternative cannot be determined.
inline DataTypePtr constGeoStoredType(const ActionsDAG::Node & node)
{
    if (!node.result_type)
        return nullptr;

    const auto * variant_type = typeid_cast<const DataTypeVariant *>(node.result_type.get());
    const auto * dynamic_type = variant_type ? nullptr : typeid_cast<const DataTypeDynamic *>(node.result_type.get());
    if ((!variant_type && !dynamic_type) || !node.column)
        return nullptr;

    const IColumn * data_col = node.column.get();
    if (const auto * const_col = typeid_cast<const ColumnConst *>(data_col))
        data_col = &const_col->getDataColumn();

    if (dynamic_type)
    {
        const auto * dynamic_col = typeid_cast<const ColumnDynamic *>(data_col);
        if (!dynamic_col || dynamic_col->size() != 1)
            return nullptr;

        return dynamic_col->getTypeAt(0);
    }

    const auto * variant_col = typeid_cast<const ColumnVariant *>(data_col);
    if (!variant_col || variant_col->size() != 1)
        return nullptr;

    const auto discr = variant_col->globalDiscriminatorAt(0);
    if (discr == ColumnVariant::NULL_DISCRIMINATOR || discr >= variant_type->getVariants().size())
        return nullptr;

    return variant_type->getVariants()[discr];
}

inline std::string constGeoKindName(const ActionsDAG::Node & node)
{
    const auto stored_type = constGeoStoredType(node);
    if (stored_type)
        return geoKindNameOfType(*stored_type);
    if (typeid_cast<const DataTypeVariant *>(node.result_type.get())
        || typeid_cast<const DataTypeDynamic *>(node.result_type.get()))
        return {};
    return geoKindNameOfType(*node.result_type);
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

    /// Any argument besides the single accepted geometry column input and constant geometry
    /// literals (e.g. a second column, or a non-constant expression) means the predicate's
    /// truth can depend on something the bbox can't see.
    const ActionsDAG::Node * input_child = nullptr;
    bool has_extra_non_constant = false;
    bool any_extraction_failed = false;
    bool any_kind_rejected = false;
    bool any_deferred_kind_rejection = false;
    /// Paired with the argument position it came from, needed by `treatsConstTupleAsPoint` below
    /// to tell `pointInPolygon`'s own point argument apart from its polygon-component arguments.
    struct ConstGeoField
    {
        size_t arg_index = 0;
        Field field;
    };
    std::vector<ConstGeoField> const_fields;
    /// Whether evaluating this predicate actually runs a topology check on its constant geometry
    /// argument(s) at all (e.g. `pointInPolygon`'s `validate_polygons` setting) -- see
    /// extractBboxFromFieldValue's `require_valid` parameter.
    const bool require_valid = node.function_base->requiresValidConstGeometry();

    size_t arg_index = 0;
    for (const auto * child : node.children)
    {
        const size_t this_arg_index = arg_index++;
        if (child->type == ActionsDAG::ActionType::INPUT)
        {
            /// Checked for EVERY input, accepted or not: `accept_input` rejecting a `Dynamic`/
            /// `Variant` column is precisely the case that used to fall through to
            /// `has_extra_non_constant` and let a sibling conjunct prune the exception away.
            if (child->result_type
                && hasDeferredGeometryKindRejection(*child->result_type, *node.function_base, this_arg_index))
            {
                any_deferred_kind_rejection = true;
                continue;
            }

            if (!input_child && accept_input(*child))
            {
                /// A column explicitly typed as a geometry kind this predicate is guaranteed to
                /// reject at THIS argument position (e.g. a `Point`-typed column as the polygon
                /// argument of `polygonsIntersectCartesian`, which only accepts `Ring`/`Polygon`/
                /// `MultiPolygon`) is guaranteed to raise `ILLEGAL_TYPE_OF_ARGUMENT` once evaluated
                /// on any row. Pruning row groups/granules by this column's bbox instead would
                /// silently hide that exception on any row group whose bbox happens to be disjoint
                /// from the query bbox. `constGeoKindName` returns empty for a `Variant`/`Dynamic`
                /// column (its concrete per-row kind isn't visible from just this `ActionsDAG`
                /// node) -- that's fine, `accept_input` already excludes those columns separately,
                /// since a single declared kind can't be checked against a column that may store a
                /// different kind in every row.
                const auto kind_name = constGeoKindName(*child);
                if (!kind_name.empty() && node.function_base->rejectsColumnGeometryKind(kind_name, this_arg_index))
                {
                    any_kind_rejected = true;
                    continue;
                }
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

        /// A constant explicitly typed as a geometry kind this predicate is guaranteed to reject
        /// (e.g. a `Point`/`LineString`/`MultiPoint`/`MultiLineString` constant for
        /// `polygonsIntersectCartesian`, which only accepts `Ring`/`Polygon`/`MultiPolygon`) is
        /// checked here, before its `Field` is ever interpreted by shape below -- shape alone
        /// can't tell it apart from an accepted kind (`Ring`/`LineString`/`MultiPoint` all flatten
        /// to the same `Array(Point)`-shaped `Field`; `Polygon`/`MultiLineString` both flatten to
        /// `Array(Array(Point))`), so the explicit kind name, read from the `DataType`/discriminator,
        /// is the only way to know evaluation is guaranteed to raise `ILLEGAL_TYPE_OF_ARGUMENT`.
        /// This must fail closed like a structural extraction failure below, not be silently
        /// dropped as "not geometry-shaped": a predicate that genuinely doesn't know whether it
        /// accepts a given kind (e.g. a WASM UDF) never reaches here, since
        /// `rejectsConstGeometryKind` defaults to false for it. Uses the position-aware
        /// `rejectsColumnGeometryKind` (not `rejectsConstGeometryKind` directly), for the same
        /// reason as the column check above: `pointInPolygon`'s first argument legitimately
        /// accepts an explicitly `Point`-typed constant, exactly as it does an explicitly
        /// `Point`-typed column.
        const auto kind_name = constGeoKindName(*child);
        if (!kind_name.empty() && node.function_base->rejectsColumnGeometryKind(kind_name, this_arg_index))
        {
            any_kind_rejected = true;
            continue;
        }

        /// A `Dynamic`/`Variant` constant whose stored alternative carries no kind name -- say
        /// `CAST((500., 500.) AS Tuple(Float64, Float64))::Dynamic` -- is an unknown kind, not an
        /// absent one, exactly as for a column (see `hasDeferredGeometryKindRejection`). The
        /// structural shape below can't stand in for the missing name here: `tryExtractConstGeoField`
        /// flattens the constant to a raw tuple that `extractBboxFromFieldValue` declines without
        /// poisoning `acc.valid`, so the node would be downgraded to `NotApplicable` -- while the
        /// per-row overload `ExecutableFunctionDynamicAdaptor`/`ExecutableFunctionVariantAdaptor`
        /// builds resolves that same tuple as a `Point` through `callOnGeometryDataType` and raises.
        /// Pruned away by a sibling conjunct, that exception never surfaces. Fail closed instead.
        /// Unless the stored alternative resolves structurally after all -- `constGeoStoredType`
        /// gives the concrete type whose overload will be built, and `structuralGeoKindName` reads
        /// the raw tuple as a `Point` exactly as `callOnGeometryDataType` does -- in which case the
        /// predicate can be asked about that kind directly, and only a kind it rejects at this
        /// position fails closed.
        if (kind_name.empty() && child->result_type && GeoBboxDetail::isDeferredGeometryKindType(*child->result_type))
        {
            const auto stored_type = constGeoStoredType(*child);
            const bool rejected = stored_type
                ? GeoBboxDetail::rejectsUnnamedGeometryType(*stored_type, *node.function_base, this_arg_index)
                : GeoBboxDetail::rejectsAnyGeometryKind(*node.function_base, this_arg_index);
            if (rejected)
            {
                any_deferred_kind_rejection = true;
                continue;
            }
        }

        const_fields.push_back({this_arg_index, std::move(field)});
    }

    /// A constant geometry argument that fails to extract, or is explicitly typed as a kind this
    /// predicate is guaranteed to reject, is guaranteed to raise on evaluation, regardless of
    /// whether an accepted-column input is present. This -- and every validation below -- must be
    /// checked unconditionally, before `!input_child` or `has_extra_non_constant` are allowed to
    /// downgrade the result to `NotApplicable`/`NoInfo`: with
    /// `short_circuit_function_evaluation = 'disable'` this node is still evaluated on every row
    /// even when it doesn't reference the accepted column (e.g. a sibling `and` conjunct on a
    /// *different* geometry column), so an invalid constant geometry here must still veto pruning
    /// for the whole conjunction -- otherwise pruning could proceed using only an unrelated,
    /// valid conjunct's bbox, silently hiding the exception this conjunct is guaranteed to raise.
    if (any_extraction_failed || any_kind_rejected || any_deferred_kind_rejection)
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
                entry.field, field_acc, require_valid, node.function_base->treatsConstTupleAsPoint(entry.arg_index));
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
            const_fields[0].field, acc, require_valid, node.function_base->treatsConstTupleAsPoint(const_fields[0].arg_index));
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
