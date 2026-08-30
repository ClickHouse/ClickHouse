#pragma once

#include <DataTypes/IDataType.h>

#include <Functions/geometry.h>

#include <optional>
#include <vector>


namespace DB
{

class IColumn;
class ReadBuffer;
class WriteBuffer;

static constexpr UInt8 GEO_SERDE_VERSION = 1;

static constexpr size_t MAX_POINTS_IN_CONVEX_HULL_STATE = 100'000'000;
static constexpr size_t MAX_POINTS_IN_POLYGONAL_STATE = 10'000'000;

/// Cumulative allocation budget threaded through polygonal-state deserialization.
struct PolygonalStateBudget
{
    size_t points = 0;
    size_t rings = 0;
};


/// Maps global variant discriminator index -> GeometryColumnType.
using VariantTypeMap = std::vector<GeometryColumnType>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

/// A geometry value resolved to its physical nested column and row.
struct GeometryColumnValue
{
    const IColumn * column = nullptr;
    size_t row_num = 0;
    GeometryColumnType type = GeometryColumnType::Null;
};

/// Traversal state for bounded convex-hull ingestion. A cursor is valid only for
/// repeated calls for the same geometry row.
struct ConvexHullPointsCursor
{
    size_t component_index = 0;
    size_t point_index = 0;
    bool initialized = false;
    bool finished = false;
};


std::optional<GeometryColumnType> getUnambiguousGeometryColumnTypeFromDataType(const DataTypePtr & type, const String & function_name);

VariantTypeMap buildVariantTypeMap(const DataTypePtr & argument_type);

/// Unwrap a constant or Geometry (Variant) row without materializing it as a Field.
GeometryColumnValue getGeometryColumnValue(
    const IColumn * column, size_t row_num, GeometryColumnType declared_type, bool is_variant, const VariantTypeMap & type_map);

/// `Geometry` preserves distinct active types even when their underlying arrays have the same
/// shape. Polygonal aggregates deliberately accept an active `LineString` as a `Ring` and an
/// active `MultiLineString` as a `Polygon`; this normalization is only for `Geometry`, not for
/// typed columns.
GeometryColumnType normalizePolygonalVariantType(GeometryColumnType type);

void normalizeAndValidatePolygonalResult(CartesianMultiPolygon & geometry, const char * function_name);

size_t countMultiPolygonPoints(const CartesianMultiPolygon & mp);
size_t recountPolygonalPointsAndCheck(
    const std::vector<CartesianMultiPolygon> & chunks, // STYLE_CHECK_ALLOW_STD_CONTAINERS
    const char * function_name);

void serializeGeoRing(const CartesianRing & ring, WriteBuffer & buf);
void serializeGeoPolygon(const CartesianPolygon & poly, WriteBuffer & buf);
void serializeGeoMultiPolygon(const CartesianMultiPolygon & mp, WriteBuffer & buf);

CartesianRing deserializeGeoRing(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget);
CartesianPolygon deserializeGeoPolygon(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget);
CartesianMultiPolygon deserializeGeoMultiPolygon(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget);
void validateDeserializedMultiPolygon(CartesianMultiPolygon & mp, const char * function_name);

void insertMultiPolygonIntoColumn(const CartesianMultiPolygon & mp, IColumn & to);

/// Convert one typed or already-resolved Variant value directly from its physical columns.
/// Empty geometry becomes an empty result (neutral for union, absorbing for intersection).
CartesianMultiPolygon columnToMultiPolygon(const IColumn & column, size_t row_num, GeometryColumnType geo_type, const char * function_name);

/// The first call validates the complete geometry row before changing `out`. Each
/// call then appends at most `max_batch_points` points that contribute to its hull.
size_t appendConvexHullPointsBatchFromColumn(
    const IColumn & column,
    size_t row_num,
    GeometryColumnType geo_type,
    CartesianMultiPoint & out,
    ConvexHullPointsCursor & cursor,
    size_t max_batch_points,
    const char * function_name);

}
