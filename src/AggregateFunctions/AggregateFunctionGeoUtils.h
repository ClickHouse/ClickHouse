#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeVariant.h>

#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <cmath>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int INCORRECT_DATA;
}

static constexpr UInt8 GEO_SERDE_VERSION = 1;

/// Deserialization guards.
static constexpr size_t MIN_POINTS_PER_POLYGONAL_RING = 4;
static constexpr size_t MAX_POINTS_PER_RING = 10'000'000;
static constexpr size_t MAX_POINTS_IN_CONVEX_HULL_STATE = 100'000'000;
static constexpr size_t MAX_POINTS_IN_POLYGONAL_STATE = 10'000'000;
/// Cumulative cap on the number of polygons and rings allocated while deserializing a single
/// polygonal state. Individual containers (the polygon count of a `MultiPolygon`, the inner-ring
/// count of a `Polygon`) are deliberately not capped on their own: any shape the aggregate can
/// produce within the point budget must round-trip through serialization, and a union of many
/// disjoint polygons can legitimately hold far more than a few thousand components. This
/// cumulative cap is a cheap early guard: it is charged before every container is read, and the
/// containers are filled incrementally. It is not the primary bound, though — on its own it would
/// still let a metadata-heavy payload allocate more rings than the writer can produce. A valid
/// writer-emitted polygonal ring has at least `MIN_POINTS_PER_POLYGONAL_RING` points, so derive the
/// ring budget from the point budget and reject shorter rings before materializing them.
static constexpr size_t MAX_RINGS_IN_POLYGONAL_STATE = MAX_POINTS_IN_POLYGONAL_STATE / MIN_POINTS_PER_POLYGONAL_RING;

/// Cumulative allocation budget threaded through polygonal-state deserialization.
struct PolygonalStateBudget
{
    size_t points = 0;
    size_t rings = 0;
};


/// Maps global variant discriminator index -> GeometryColumnType.
using VariantTypeMap = std::vector<GeometryColumnType>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

inline std::optional<GeometryColumnType>
getUnambiguousGeometryColumnTypeFromDataType(const DataTypePtr & type, const String & function_name)
{
    auto geo_type = getGeometryColumnTypeFromDataType(type);
    if (!geo_type)
        return std::nullopt;

    const auto & type_name = type->getName();
    if (*geo_type == GeometryColumnType::Ring && type_name != "Ring")
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} has type {} which is ambiguous between Ring and LineString. "
            "Cast it to an explicit geometry type.",
            function_name,
            type_name);

    if (*geo_type == GeometryColumnType::Polygon && type_name != "Polygon")
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} has type {} which is ambiguous between Polygon and MultiLineString. "
            "Cast it to an explicit geometry type.",
            function_name,
            type_name);

    return geo_type;
}

inline VariantTypeMap buildVariantTypeMap(const DataTypePtr & argument_type)
{
    const auto * variant_type = typeid_cast<const DataTypeVariant *>(argument_type.get());
    if (!variant_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected Geometry (Variant) type, got {}", argument_type->getName());

    const auto & variants = variant_type->getVariants();
    VariantTypeMap map;
    map.reserve(variants.size());
    for (const auto & v : variants)
    {
        auto geo = getGeometryColumnTypeFromDataType(v);
        if (!geo)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unrecognized variant type {} in Geometry", v->getName());
        map.push_back(*geo);
    }
    return map;
}


/// Resolve GeometryColumnType for a row. Handles ColumnConst. Returns Null for NULL rows.
inline GeometryColumnType resolveGeometryVariantType(const IColumn * column, size_t row_num, const VariantTypeMap & type_map)
{
    const ColumnVariant * col_variant = typeid_cast<const ColumnVariant *>(column);
    size_t variant_row = row_num;

    if (!col_variant)
    {
        if (const auto * col_const = typeid_cast<const ColumnConst *>(column))
        {
            col_variant = typeid_cast<const ColumnVariant *>(&col_const->getDataColumn());
            variant_row = 0;
        }
    }

    if (!col_variant)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a Geometry (Variant) column");

    auto global_discr = col_variant->globalDiscriminatorAt(variant_row);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return GeometryColumnType::Null;

    if (global_discr >= type_map.size())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Geometry variant discriminator {} out of range ({})",
            static_cast<int>(global_discr),
            type_map.size());

    return type_map[global_discr];
}


/// `Geometry` preserves distinct active types even when their underlying arrays have the same
/// shape. Polygonal aggregates deliberately accept an active `LineString` as a `Ring` and an
/// active `MultiLineString` as a `Polygon`; this normalization is only for `Geometry`, not for
/// typed columns.
inline GeometryColumnType normalizePolygonalVariantType(GeometryColumnType t)
{
    if (t == GeometryColumnType::Linestring)
        return GeometryColumnType::Ring;
    if (t == GeometryColumnType::MultiLinestring)
        return GeometryColumnType::Polygon;
    return t;
}


inline void validateFinitePoint(const CartesianPoint & point, const char * function_name)
{
    Float64 x = point.get<0>();
    Float64 y = point.get<1>();

    if (std::isnan(x) || std::isnan(y))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of aggregate function {} must not contain NaN coordinates", function_name);
    if (std::isinf(x) || std::isinf(y))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Argument of aggregate function {} must not contain infinite coordinates", function_name);
}

inline void validateFinitePolygon(const CartesianPolygon & polygon, const char * function_name)
{
    for (const auto & point : polygon.outer())
        validateFinitePoint(point, function_name);

    for (const auto & inner : polygon.inners())
        for (const auto & point : inner)
            validateFinitePoint(point, function_name);
}

inline void validateValidPolygonalGeometry(const CartesianMultiPolygon & geometry, const char * function_name)
{
    String reason;
    if (!boost::geometry::is_valid(geometry, reason))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Argument of aggregate function {} must be a valid polygonal geometry: {}", function_name, reason);
}

inline void normalizeAndValidatePolygonalResult(CartesianMultiPolygon & geometry, const char * function_name)
{
    auto validate_result_point = [function_name](const CartesianPoint & point)
    {
        if (!std::isfinite(point.get<0>()) || !std::isfinite(point.get<1>()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} produced a non-finite coordinate ({}, {}) during reduction",
                function_name,
                point.get<0>(),
                point.get<1>());
    };

    for (auto & polygon : geometry)
    {
        if (polygon.outer().empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} produced a polygon with an empty outer ring during reduction",
                function_name);
        for (const auto & point : polygon.outer())
            validate_result_point(point);
        for (const auto & inner : polygon.inners())
            for (const auto & point : inner)
                validate_result_point(point);
        boost::geometry::correct(polygon);
    }

    String reason;
    if (!boost::geometry::is_valid(geometry, reason))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Aggregate function {} produced invalid polygonal geometry during reduction: {}",
            function_name,
            reason);
}


inline size_t countMultiPolygonPoints(const CartesianMultiPolygon & mp)
{
    size_t total = 0;
    for (const auto & poly : mp)
    {
        total += poly.outer().size();
        for (const auto & inner : poly.inners())
            total += inner.size();
    }
    return total;
}

/// Recompute the total point count of a polygonal state from its normalized chunks and
/// re-enforce the budget. `boost::geometry::correct` (run during deserialization and the
/// add path) can append closing points that were not charged when the count was first
/// accumulated, so the trusted in-memory count must be derived from the geometry itself.
inline size_t recountPolygonalPointsAndCheck(
    const std::vector<CartesianMultiPolygon> & chunks, // STYLE_CHECK_ALLOW_STD_CONTAINERS
    const char * function_name)
{
    size_t total = 0;
    for (const auto & chunk : chunks)
        total += countMultiPolygonPoints(chunk);
    if (total > MAX_POINTS_IN_POLYGONAL_STATE)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: total points {} exceed the limit {} after normalization",
            function_name,
            total,
            MAX_POINTS_IN_POLYGONAL_STATE);
    return total;
}

inline void serializeGeoRing(const CartesianRing & ring, WriteBuffer & buf)
{
    writeVarUInt(ring.size(), buf);
    for (const auto & pt : ring)
    {
        writeBinaryLittleEndian(pt.get<0>(), buf);
        writeBinaryLittleEndian(pt.get<1>(), buf);
    }
}

inline void serializeGeoPolygon(const CartesianPolygon & poly, WriteBuffer & buf)
{
    serializeGeoRing(poly.outer(), buf);
    writeVarUInt(poly.inners().size(), buf);
    for (const auto & inner : poly.inners())
        serializeGeoRing(inner, buf);
}

inline void serializeGeoMultiPolygon(const CartesianMultiPolygon & mp, WriteBuffer & buf)
{
    writeVarUInt(mp.size(), buf);
    for (const auto & poly : mp)
        serializeGeoPolygon(poly, buf);
}


inline CartesianRing deserializeGeoRing(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget)
{
    UInt64 size = 0;
    readVarUInt(size, buf);

    /// The writer only serializes corrected, valid polygonal rings, which have at least four
    /// points. Reject shorter rings before materializing them. Besides keeping reader and writer
    /// representations aligned, this makes every allocated ring consume the same minimum share of
    /// the cumulative point budget used to derive `MAX_RINGS_IN_POLYGONAL_STATE`.
    if (size < MIN_POINTS_PER_POLYGONAL_RING)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: ring has {} points (minimum {})",
            function_name,
            size,
            MIN_POINTS_PER_POLYGONAL_RING);

    if (size > MAX_POINTS_PER_RING)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: ring has {} points (limit {})",
            function_name,
            size,
            MAX_POINTS_PER_RING);

    if (size > MAX_POINTS_IN_POLYGONAL_STATE - budget.points)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: total points exceed polygonal state budget {}",
            function_name,
            MAX_POINTS_IN_POLYGONAL_STATE);
    budget.points += size;

    /// Do not reserve/resize the ring to the declared count up front. A corrupted state can
    /// advertise an in-range count (up to `MAX_POINTS_PER_RING`) yet carry a truncated coordinate
    /// payload, so `ring.resize(size)` would allocate the full ring (up to ~160 MiB) before
    /// `readBinaryLittleEndian` hits EOF. Append points incrementally with a bounded initial
    /// reservation; `push_back` then grows the ring geometrically as points are actually read, so
    /// memory grows only with the bytes present in the serialized state, not the declared count.
    static constexpr UInt64 deserialize_ring_batch = 4096;
    CartesianRing ring;
    ring.reserve(size < deserialize_ring_batch ? static_cast<size_t>(size) : deserialize_ring_batch);
    for (UInt64 i = 0; i < size; ++i)
    {
        Float64 x = 0;
        Float64 y = 0;
        readBinaryLittleEndian(x, buf);
        readBinaryLittleEndian(y, buf);
        if (!std::isfinite(x) || !std::isfinite(y))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted state of aggregate function {}: non-finite coordinate ({}, {})",
                function_name,
                x,
                y);
        ring.push_back(CartesianPoint(x, y));
    }
    return ring;
}

inline void chargeRingBudget(UInt64 count, const char * function_name, PolygonalStateBudget & budget)
{
    if (count > MAX_RINGS_IN_POLYGONAL_STATE - budget.rings)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: total rings exceed polygonal state budget {}",
            function_name,
            MAX_RINGS_IN_POLYGONAL_STATE);
    budget.rings += count;
}

inline CartesianPolygon deserializeGeoPolygon(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget)
{
    CartesianPolygon poly;
    poly.outer() = deserializeGeoRing(buf, function_name, budget);

    UInt64 inner_count = 0;
    readVarUInt(inner_count, buf);

    /// The inner-ring count is not capped on its own: a polygon the aggregate produces may hold
    /// any number of holes within the point budget, and such a state must round-trip. Bound the
    /// allocation with the cumulative ring budget instead, then read the inner rings incrementally
    /// (bounded initial reservation + `push_back`) so a truncated payload that advertises a large
    /// count but carries no ring bytes cannot pre-allocate storage proportional to that count.
    chargeRingBudget(inner_count, function_name, budget);
    static constexpr UInt64 deserialize_ring_batch = 4096;
    auto & inners = poly.inners();
    inners.reserve(inner_count < deserialize_ring_batch ? static_cast<size_t>(inner_count) : deserialize_ring_batch);
    for (UInt64 i = 0; i < inner_count; ++i)
        inners.push_back(deserializeGeoRing(buf, function_name, budget));
    return poly;
}

inline CartesianMultiPolygon deserializeGeoMultiPolygon(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget)
{
    UInt64 poly_count = 0;
    readVarUInt(poly_count, buf);

    if (poly_count == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted state of aggregate function {}: empty multipolygon chunk", function_name);

    /// The per-multipolygon polygon count is not capped on its own: a union of many disjoint
    /// polygons can legitimately produce far more than a few thousand components, and such a state
    /// must round-trip. Bound the allocation with the cumulative ring budget instead (each polygon
    /// contributes at least its outer ring), then read the polygons incrementally (bounded initial
    /// reservation + `push_back`) so a truncated payload that advertises a large count but carries
    /// no polygon bytes cannot pre-allocate storage proportional to that count.
    chargeRingBudget(poly_count, function_name, budget);
    static constexpr UInt64 deserialize_polygon_batch = 4096;
    CartesianMultiPolygon mp;
    mp.reserve(poly_count < deserialize_polygon_batch ? static_cast<size_t>(poly_count) : deserialize_polygon_batch);
    for (UInt64 i = 0; i < poly_count; ++i)
        mp.push_back(deserializeGeoPolygon(buf, function_name, budget));
    return mp;
}


/// Re-establish the same polygon validity invariant enforced by the add path
/// (boost::geometry::correct + is_valid). Malformed serialized states that
/// passed finite-coordinate checks but carry self-intersecting or otherwise
/// invalid topology are rejected here.
inline void validateDeserializedMultiPolygon(CartesianMultiPolygon & mp, const char * function_name)
{
    for (auto & poly : mp)
        boost::geometry::correct(poly);

    String reason;
    if (!boost::geometry::is_valid(mp, reason))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: deserialized geometry is invalid: {}",
            function_name,
            reason);
}


inline void insertMultiPolygonIntoColumn(const CartesianMultiPolygon & mp, IColumn & to)
{
    auto & mp_arr = assert_cast<ColumnArray &>(to);
    auto & mp_offsets = mp_arr.getOffsets();
    auto & poly_arr = assert_cast<ColumnArray &>(mp_arr.getData());
    auto & poly_offsets = poly_arr.getOffsets();
    auto & ring_arr = assert_cast<ColumnArray &>(poly_arr.getData());
    auto & ring_offsets = ring_arr.getOffsets();
    auto & tuple_col = assert_cast<ColumnTuple &>(ring_arr.getData());
    auto & x_col = assert_cast<ColumnFloat64 &>(tuple_col.getColumn(0));
    auto & y_col = assert_cast<ColumnFloat64 &>(tuple_col.getColumn(1));

    for (const auto & poly : mp)
    {
        for (const auto & pt : poly.outer())
        {
            x_col.getData().push_back(pt.get<0>());
            y_col.getData().push_back(pt.get<1>());
        }
        ring_offsets.push_back(x_col.getData().size());

        for (const auto & inner : poly.inners())
        {
            for (const auto & pt : inner)
            {
                x_col.getData().push_back(pt.get<0>());
                y_col.getData().push_back(pt.get<1>());
            }
            ring_offsets.push_back(x_col.getData().size());
        }

        poly_offsets.push_back(ring_offsets.size());
    }

    mp_offsets.push_back(poly_offsets.size());
}


/// Empty geometry → empty result (neutral for union, absorbing for intersect).
inline CartesianMultiPolygon fieldToMultiPolygon(const Field & field, GeometryColumnType geo_type, const char * function_name)
{
    CartesianMultiPolygon result;

    switch (geo_type)
    {
        case GeometryColumnType::Ring: {
            auto ring = getRingFromField<CartesianPoint>(field);
            if (ring.empty())
                break;
            CartesianPolygon poly;
            poly.outer() = std::move(ring);
            validateFinitePolygon(poly, function_name);
            boost::geometry::correct(poly);
            result.push_back(std::move(poly));
            break;
        }
        case GeometryColumnType::Polygon: {
            auto poly = getPolygonFromField<CartesianPoint>(field);
            if (poly.outer().empty())
            {
                /// An empty outer ring is treated as an empty geometry, but only when the whole
                /// polygon is empty. A polygon with an empty outer and non-empty inner rings is
                /// malformed and must not bypass finite-coordinate / validity checks.
                if (!poly.inners().empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Argument of aggregate function {} has a polygon with an empty outer ring but non-empty inner rings",
                        function_name);
                break;
            }
            validateFinitePolygon(poly, function_name);
            boost::geometry::correct(poly);
            result.push_back(std::move(poly));
            break;
        }
        case GeometryColumnType::MultiPolygon: {
            result = getMultiPolygonFromField<CartesianPoint>(field);
            for (const auto & poly : result)
                if (poly.outer().empty() && !poly.inners().empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Argument of aggregate function {} has a polygon with an empty outer ring but non-empty inner rings",
                        function_name);
            std::erase_if(result, [](const CartesianPolygon & p) { return p.outer().empty(); });
            for (auto & poly : result)
            {
                validateFinitePolygon(poly, function_name);
                boost::geometry::correct(poly);
            }
            break;
        }
        case GeometryColumnType::Point:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} does not accept Point arguments", function_name);
        case GeometryColumnType::Linestring:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} does not accept LineString arguments", function_name);
        case GeometryColumnType::MultiLinestring:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} does not accept MultiLineString arguments", function_name);
        case GeometryColumnType::Null: break;
    }

    if (!result.empty())
        validateValidPolygonalGeometry(result, function_name);

    return result;
}

}
