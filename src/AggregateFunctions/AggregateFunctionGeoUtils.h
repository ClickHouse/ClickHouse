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
static constexpr size_t MAX_POINTS_PER_RING = 10'000'000;
static constexpr size_t MAX_RINGS_PER_POLYGON = 10'000;
static constexpr size_t MAX_POLYGONS_PER_MULTIPOLYGON = 10'000;
static constexpr size_t MAX_CHUNKS_PER_STATE = 10'000;
static constexpr size_t MAX_POINTS_IN_CONVEX_HULL_STATE = 100'000'000;
static constexpr size_t MAX_POINTS_IN_POLYGONAL_STATE = 10'000'000;
/// Cumulative cap on the number of polygons and rings allocated while deserializing a
/// single polygonal state. The per-container limits (`MAX_POLYGONS_PER_MULTIPOLYGON`,
/// `MAX_RINGS_PER_POLYGON`) bound each `resize` individually, but their product across
/// chunks/polygons can still amplify a metadata-only payload (every ring of size `0`)
/// into a huge allocation while the point budget stays at zero. This cap is checked
/// before every metadata `resize` so such payloads are rejected early.
static constexpr size_t MAX_RINGS_IN_POLYGONAL_STATE = 10'000'000;

/// Cumulative allocation budget threaded through polygonal-state deserialization.
struct PolygonalStateBudget
{
    size_t points = 0;
    size_t rings = 0;
};


/// Maps global variant discriminator index -> GeometryColumnType.
using VariantTypeMap = std::vector<GeometryColumnType>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

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


/// In Geometry (Variant), Ring/LineString and Polygon/MultiLineString share discriminators
/// (structurally identical). Normalize to polygonal equivalents. Not for typed columns.
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

    CartesianRing ring;
    ring.resize(size);
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
        ring[i] = CartesianPoint(x, y);
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
    if (inner_count > MAX_RINGS_PER_POLYGON)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: polygon has {} inner rings (limit {})",
            function_name,
            inner_count,
            MAX_RINGS_PER_POLYGON);

    /// Bound metadata allocation before reserving ring storage.
    chargeRingBudget(inner_count, function_name, budget);
    poly.inners().resize(inner_count);
    for (UInt64 i = 0; i < inner_count; ++i)
        poly.inners()[i] = deserializeGeoRing(buf, function_name, budget);
    return poly;
}

inline CartesianMultiPolygon deserializeGeoMultiPolygon(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget)
{
    UInt64 poly_count = 0;
    readVarUInt(poly_count, buf);
    if (poly_count > MAX_POLYGONS_PER_MULTIPOLYGON)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: multi-polygon has {} polygons (limit {})",
            function_name,
            poly_count,
            MAX_POLYGONS_PER_MULTIPOLYGON);

    /// Bound metadata allocation before reserving polygon storage.
    chargeRingBudget(poly_count, function_name, budget);
    CartesianMultiPolygon mp;
    mp.resize(poly_count);
    for (UInt64 i = 0; i < poly_count; ++i)
        mp[i] = deserializeGeoPolygon(buf, function_name, budget);
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
        case GeometryColumnType::Null:
            break;
    }

    if (!result.empty())
        validateValidPolygonalGeometry(result, function_name);

    return result;
}

}
