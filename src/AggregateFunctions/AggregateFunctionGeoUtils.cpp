#include <AggregateFunctions/AggregateFunctionGeoUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeVariant.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <boost/geometry.hpp>

#include <algorithm>
#include <cmath>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int INCORRECT_DATA;
}

namespace
{

/// Deserialization guards local to the implementation.
constexpr size_t MIN_POINTS_PER_POLYGONAL_RING = 4;
constexpr size_t MAX_POINTS_PER_RING = 10'000'000;
/// A valid writer-emitted ring has at least four points, so this cumulative
/// metadata budget is derived from the point budget.
constexpr size_t MAX_RINGS_IN_POLYGONAL_STATE = MAX_POINTS_IN_POLYGONAL_STATE / MIN_POINTS_PER_POLYGONAL_RING;

struct ArrayRange
{
    const IColumn & data;
    size_t begin;
    size_t end;
};


struct PointColumns
{
    const ColumnFloat64::Container & x;
    const ColumnFloat64::Container & y;
};


ArrayRange getArrayRange(const IColumn & column, size_t row_num)
{
    const auto & array = assert_cast<const ColumnArray &>(column);
    const auto & offsets = array.getOffsets();
    const size_t begin = row_num == 0 ? 0 : offsets[row_num - 1];
    return {array.getData(), begin, offsets[row_num]};
}


PointColumns getPointColumns(const IColumn & column)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(column);
    const auto & x = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & y = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();
    return {x, y};
}


CartesianPoint getPointFromColumns(const PointColumns & columns, size_t row_num)
{
    return {columns.x[row_num], columns.y[row_num]};
}


void validateFinitePoint(const CartesianPoint & point, const char * function_name)
{
    const Float64 x = point.get<0>();
    const Float64 y = point.get<1>();

    if (std::isnan(x) || std::isnan(y))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of aggregate function {} must not contain NaN coordinates", function_name);
    if (std::isinf(x) || std::isinf(y))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Argument of aggregate function {} must not contain infinite coordinates", function_name);
}


void validateValidPolygonalGeometry(const CartesianMultiPolygon & geometry, const char * function_name)
{
    String reason;
    if (!boost::geometry::is_valid(geometry, reason))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Argument of aggregate function {} must be a valid polygonal geometry: {}", function_name, reason);
}


void validatePointRange(const ArrayRange & range, const char * function_name)
{
    const auto columns = getPointColumns(range.data);
    for (size_t i = range.begin; i < range.end; ++i)
        validateFinitePoint(getPointFromColumns(columns, i), function_name);
}


size_t appendPointRangeBatch(
    const ArrayRange & range, size_t & point_index, size_t max_points, CartesianMultiPoint & out)
{
    const size_t count = std::min(max_points, range.end - point_index);
    const auto columns = getPointColumns(range.data);
    out.reserve(out.size() + count);
    const size_t batch_end = point_index + count;
    for (size_t i = point_index; i < batch_end; ++i)
        out.push_back(getPointFromColumns(columns, i));
    point_index = batch_end;
    return count;
}


CartesianRing getRingFromColumn(const IColumn & column, size_t row_num, const char * function_name)
{
    const auto range = getArrayRange(column, row_num);
    const auto columns = getPointColumns(range.data);
    CartesianRing ring;
    ring.reserve(range.end - range.begin);
    for (size_t i = range.begin; i < range.end; ++i)
    {
        auto point = getPointFromColumns(columns, i);
        validateFinitePoint(point, function_name);
        ring.push_back(std::move(point));
    }
    return ring;
}


CartesianPolygon getPolygonFromColumn(const IColumn & column, size_t row_num, const char * function_name)
{
    const auto rings = getArrayRange(column, row_num);
    CartesianPolygon polygon;
    if (rings.begin == rings.end)
        return polygon;

    polygon.outer() = getRingFromColumn(rings.data, rings.begin, function_name);
    polygon.inners().reserve(rings.end - rings.begin - 1);
    for (size_t i = rings.begin + 1; i < rings.end; ++i)
        polygon.inners().push_back(getRingFromColumn(rings.data, i, function_name));
    return polygon;
}


[[noreturn]] void throwEmptyOuterWithInner(const char * function_name)
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Argument of aggregate function {} has a polygon with an empty outer ring but non-empty inner rings",
        function_name);
}


void validatePolygonColumn(const IColumn & column, size_t row_num, const char * function_name)
{
    const auto rings = getArrayRange(column, row_num);
    if (rings.begin == rings.end)
        return;

    const auto outer = getArrayRange(rings.data, rings.begin);
    if (outer.begin == outer.end && rings.end - rings.begin > 1)
        throwEmptyOuterWithInner(function_name);

    validatePointRange(outer, function_name);
    for (size_t i = rings.begin + 1; i < rings.end; ++i)
        validatePointRange(getArrayRange(rings.data, i), function_name);
}


std::optional<ArrayRange> getPolygonOuterRange(const IColumn & column, size_t row_num)
{
    const auto rings = getArrayRange(column, row_num);
    if (rings.begin == rings.end)
        return std::nullopt;
    return getArrayRange(rings.data, rings.begin);
}


void advanceToNextNonEmptyLine(const ArrayRange & lines, ConvexHullPointsCursor & cursor)
{
    while (cursor.component_index < lines.end)
    {
        const auto points = getArrayRange(lines.data, cursor.component_index);
        cursor.point_index = points.begin;
        if (points.begin != points.end)
            return;
        ++cursor.component_index;
    }
    cursor.finished = true;
}


void advanceToNextNonEmptyPolygonOuter(const ArrayRange & polygons, ConvexHullPointsCursor & cursor)
{
    while (cursor.component_index < polygons.end)
    {
        const auto outer = getPolygonOuterRange(polygons.data, cursor.component_index);
        if (outer && outer->begin != outer->end)
        {
            cursor.point_index = outer->begin;
            return;
        }
        ++cursor.component_index;
    }
    cursor.finished = true;
}

}


std::optional<GeometryColumnType> getUnambiguousGeometryColumnTypeFromDataType(const DataTypePtr & type, const String & function_name)
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


VariantTypeMap buildVariantTypeMap(const DataTypePtr & argument_type)
{
    const auto * variant_type = typeid_cast<const DataTypeVariant *>(argument_type.get());
    if (!variant_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected Geometry (Variant) type, got {}", argument_type->getName());

    const auto & variants = variant_type->getVariants();
    VariantTypeMap map;
    map.reserve(variants.size());
    for (const auto & variant : variants)
    {
        auto geo_type = getGeometryColumnTypeFromDataType(variant);
        if (!geo_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unrecognized variant type {} in Geometry", variant->getName());
        map.push_back(*geo_type);
    }
    return map;
}


GeometryColumnValue getGeometryColumnValue(
    const IColumn * column, size_t row_num, GeometryColumnType declared_type, bool is_variant, const VariantTypeMap & type_map)
{
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
    {
        column = &column_const->getDataColumn();
        row_num = 0;
    }

    if (!is_variant)
        return {column, row_num, declared_type};

    const auto * column_variant = typeid_cast<const ColumnVariant *>(column);
    if (!column_variant)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a Geometry (Variant) column");

    const auto global_discriminator = column_variant->globalDiscriminatorAt(row_num);
    if (global_discriminator == ColumnVariant::NULL_DISCRIMINATOR)
        return {};

    if (global_discriminator >= type_map.size())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Geometry variant discriminator {} out of range ({})",
            static_cast<int>(global_discriminator),
            type_map.size());

    return {
        &column_variant->getVariantByGlobalDiscriminator(global_discriminator),
        column_variant->offsetAt(row_num),
        type_map[global_discriminator]};
}


GeometryColumnType normalizePolygonalVariantType(GeometryColumnType type)
{
    if (type == GeometryColumnType::Linestring)
        return GeometryColumnType::Ring;
    if (type == GeometryColumnType::MultiLinestring)
        return GeometryColumnType::Polygon;
    return type;
}


void normalizeAndValidatePolygonalResult(CartesianMultiPolygon & geometry, const char * function_name)
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


size_t countMultiPolygonPoints(const CartesianMultiPolygon & mp)
{
    size_t total = 0;
    for (const auto & polygon : mp)
    {
        total += polygon.outer().size();
        for (const auto & inner : polygon.inners())
            total += inner.size();
    }
    return total;
}


size_t recountPolygonalPointsAndCheck(
    const std::vector<CartesianMultiPolygon> & chunks, // STYLE_CHECK_ALLOW_STD_CONTAINERS
    const char * function_name)
{
    /// `boost::geometry::correct` can append closing points that were not
    /// charged when the input was first accumulated. Derive the trusted count
    /// from the normalized geometry and re-enforce the state budget.
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


void serializeGeoRing(const CartesianRing & ring, WriteBuffer & buf)
{
    writeVarUInt(ring.size(), buf);
    for (const auto & point : ring)
    {
        writeBinaryLittleEndian(point.get<0>(), buf);
        writeBinaryLittleEndian(point.get<1>(), buf);
    }
}


void serializeGeoPolygon(const CartesianPolygon & polygon, WriteBuffer & buf)
{
    serializeGeoRing(polygon.outer(), buf);
    writeVarUInt(polygon.inners().size(), buf);
    for (const auto & inner : polygon.inners())
        serializeGeoRing(inner, buf);
}


void serializeGeoMultiPolygon(const CartesianMultiPolygon & mp, WriteBuffer & buf)
{
    writeVarUInt(mp.size(), buf);
    for (const auto & polygon : mp)
        serializeGeoPolygon(polygon, buf);
}


CartesianRing deserializeGeoRing(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget)
{
    UInt64 size = 0;
    readVarUInt(size, buf);

    /// The writer only emits corrected, valid rings. Reject shorter rings
    /// before allocation so every ring consumes the minimum share used to
    /// derive the cumulative ring budget.
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

    /// Do not resize to an untrusted declared count. A truncated payload could
    /// otherwise allocate up to the full ring limit before the first failed
    /// coordinate read. Grow incrementally from a bounded initial reservation.
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


namespace
{

void chargeRingBudget(UInt64 count, const char * function_name, PolygonalStateBudget & budget)
{
    if (count > MAX_RINGS_IN_POLYGONAL_STATE - budget.rings)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: total rings exceed polygonal state budget {}",
            function_name,
            MAX_RINGS_IN_POLYGONAL_STATE);
    budget.rings += count;
}

}


CartesianPolygon deserializeGeoPolygon(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget)
{
    CartesianPolygon polygon;
    polygon.outer() = deserializeGeoRing(buf, function_name, budget);

    UInt64 inner_count = 0;
    readVarUInt(inner_count, buf);

    /// The number of inner rings is bounded cumulatively rather than per
    /// polygon, preserving all shapes the writer can emit within the point
    /// budget. Fill incrementally to avoid trusting a truncated count.
    chargeRingBudget(inner_count, function_name, budget);

    static constexpr UInt64 deserialize_ring_batch = 4096;
    auto & inners = polygon.inners();
    inners.reserve(inner_count < deserialize_ring_batch ? static_cast<size_t>(inner_count) : deserialize_ring_batch);
    for (UInt64 i = 0; i < inner_count; ++i)
        inners.push_back(deserializeGeoRing(buf, function_name, budget));
    return polygon;
}


CartesianMultiPolygon deserializeGeoMultiPolygon(ReadBuffer & buf, const char * function_name, PolygonalStateBudget & budget)
{
    UInt64 polygon_count = 0;
    readVarUInt(polygon_count, buf);

    if (polygon_count == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted state of aggregate function {}: empty multipolygon chunk", function_name);

    /// A union may legitimately produce many disjoint polygons. Use the same
    /// cumulative ring budget as the writer and keep the initial reservation
    /// bounded for truncated payloads.
    chargeRingBudget(polygon_count, function_name, budget);
    static constexpr UInt64 deserialize_polygon_batch = 4096;
    CartesianMultiPolygon mp;
    mp.reserve(polygon_count < deserialize_polygon_batch ? static_cast<size_t>(polygon_count) : deserialize_polygon_batch);
    for (UInt64 i = 0; i < polygon_count; ++i)
        mp.push_back(deserializeGeoPolygon(buf, function_name, budget));
    return mp;
}


void validateDeserializedMultiPolygon(CartesianMultiPolygon & mp, const char * function_name)
{
    /// Restore the same corrected-and-valid invariant enforced by the add path.
    for (auto & polygon : mp)
        boost::geometry::correct(polygon);

    String reason;
    if (!boost::geometry::is_valid(mp, reason))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Corrupted state of aggregate function {}: deserialized geometry is invalid: {}",
            function_name,
            reason);
}


void insertMultiPolygonIntoColumn(const CartesianMultiPolygon & mp, IColumn & to)
{
    auto & multipolygon_array = assert_cast<ColumnArray &>(to);
    auto & multipolygon_offsets = multipolygon_array.getOffsets();
    auto & polygon_array = assert_cast<ColumnArray &>(multipolygon_array.getData());
    auto & polygon_offsets = polygon_array.getOffsets();
    auto & ring_array = assert_cast<ColumnArray &>(polygon_array.getData());
    auto & ring_offsets = ring_array.getOffsets();
    auto & tuple_column = assert_cast<ColumnTuple &>(ring_array.getData());
    auto & x_column = assert_cast<ColumnFloat64 &>(tuple_column.getColumn(0));
    auto & y_column = assert_cast<ColumnFloat64 &>(tuple_column.getColumn(1));

    for (const auto & polygon : mp)
    {
        for (const auto & point : polygon.outer())
        {
            x_column.getData().push_back(point.get<0>());
            y_column.getData().push_back(point.get<1>());
        }
        ring_offsets.push_back(x_column.getData().size());

        for (const auto & inner : polygon.inners())
        {
            for (const auto & point : inner)
            {
                x_column.getData().push_back(point.get<0>());
                y_column.getData().push_back(point.get<1>());
            }
            ring_offsets.push_back(x_column.getData().size());
        }

        polygon_offsets.push_back(ring_offsets.size());
    }

    multipolygon_offsets.push_back(polygon_offsets.size());
}


CartesianMultiPolygon columnToMultiPolygon(const IColumn & column, size_t row_num, GeometryColumnType geo_type, const char * function_name)
{
    CartesianMultiPolygon result;

    switch (geo_type)
    {
        case GeometryColumnType::Ring: {
            auto ring = getRingFromColumn(column, row_num, function_name);
            if (ring.empty())
                break;
            CartesianPolygon polygon;
            polygon.outer() = std::move(ring);
            boost::geometry::correct(polygon);
            result.push_back(std::move(polygon));
            break;
        }
        case GeometryColumnType::Polygon: {
            auto polygon = getPolygonFromColumn(column, row_num, function_name);
            if (polygon.outer().empty())
            {
                if (!polygon.inners().empty())
                    throwEmptyOuterWithInner(function_name);
                break;
            }
            boost::geometry::correct(polygon);
            result.push_back(std::move(polygon));
            break;
        }
        case GeometryColumnType::MultiPolygon: {
            const auto polygons = getArrayRange(column, row_num);
            result.reserve(polygons.end - polygons.begin);
            for (size_t i = polygons.begin; i < polygons.end; ++i)
            {
                auto polygon = getPolygonFromColumn(polygons.data, i, function_name);
                if (polygon.outer().empty())
                {
                    if (!polygon.inners().empty())
                        throwEmptyOuterWithInner(function_name);
                    continue;
                }
                boost::geometry::correct(polygon);
                result.push_back(std::move(polygon));
            }
            break;
        }
        case GeometryColumnType::Point:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} does not accept Point arguments", function_name);
        case GeometryColumnType::MultiPoint:
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} does not accept MultiPoint arguments", function_name);
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


size_t appendConvexHullPointsBatchFromColumn(
    const IColumn & column,
    size_t row_num,
    GeometryColumnType geo_type,
    CartesianMultiPoint & out,
    ConvexHullPointsCursor & cursor,
    size_t max_batch_points,
    const char * function_name)
{
    chassert(max_batch_points > 0);

    if (cursor.finished)
        return 0;

    /// Validate the complete carrier before appending its first point. In particular,
    /// an invalid coordinate in a later line or inner ring must not partially mutate
    /// the aggregate state.
    if (!cursor.initialized)
    {
        switch (geo_type)
        {
            case GeometryColumnType::Point:
                validateFinitePoint(getPointFromColumns(getPointColumns(column), row_num), function_name);
                break;
            case GeometryColumnType::MultiPoint:
            case GeometryColumnType::Ring:
            case GeometryColumnType::Linestring: {
                const auto points = getArrayRange(column, row_num);
                validatePointRange(points, function_name);
                cursor.point_index = points.begin;
                cursor.finished = points.begin == points.end;
                break;
            }
            case GeometryColumnType::MultiLinestring: {
                const auto lines = getArrayRange(column, row_num);
                for (size_t i = lines.begin; i < lines.end; ++i)
                    validatePointRange(getArrayRange(lines.data, i), function_name);
                cursor.component_index = lines.begin;
                advanceToNextNonEmptyLine(lines, cursor);
                break;
            }
            case GeometryColumnType::Polygon: {
                validatePolygonColumn(column, row_num, function_name);
                const auto outer = getPolygonOuterRange(column, row_num);
                if (outer && outer->begin != outer->end)
                    cursor.point_index = outer->begin;
                else
                    cursor.finished = true;
                break;
            }
            case GeometryColumnType::MultiPolygon: {
                const auto polygons = getArrayRange(column, row_num);
                for (size_t i = polygons.begin; i < polygons.end; ++i)
                    validatePolygonColumn(polygons.data, i, function_name);
                cursor.component_index = polygons.begin;
                advanceToNextNonEmptyPolygonOuter(polygons, cursor);
                break;
            }
            case GeometryColumnType::Null:
                cursor.finished = true;
                break;
        }
        cursor.initialized = true;
    }

    if (cursor.finished)
        return 0;

    switch (geo_type)
    {
        case GeometryColumnType::Point: {
            auto point = getPointFromColumns(getPointColumns(column), row_num);
            out.push_back(std::move(point));
            cursor.finished = true;
            return 1;
        }
        case GeometryColumnType::MultiPoint:
        case GeometryColumnType::Ring:
        case GeometryColumnType::Linestring: {
            const auto points = getArrayRange(column, row_num);
            const size_t appended = appendPointRangeBatch(points, cursor.point_index, max_batch_points, out);
            cursor.finished = cursor.point_index == points.end;
            return appended;
        }
        case GeometryColumnType::MultiLinestring: {
            const auto lines = getArrayRange(column, row_num);
            size_t appended = 0;
            while (!cursor.finished && appended < max_batch_points)
            {
                const auto points = getArrayRange(lines.data, cursor.component_index);
                appended += appendPointRangeBatch(points, cursor.point_index, max_batch_points - appended, out);
                if (cursor.point_index == points.end)
                {
                    ++cursor.component_index;
                    advanceToNextNonEmptyLine(lines, cursor);
                }
            }
            return appended;
        }
        case GeometryColumnType::Polygon: {
            const auto outer = getPolygonOuterRange(column, row_num);
            chassert(outer.has_value());
            const size_t appended = appendPointRangeBatch(*outer, cursor.point_index, max_batch_points, out);
            cursor.finished = cursor.point_index == outer->end;
            return appended;
        }
        case GeometryColumnType::MultiPolygon: {
            const auto polygons = getArrayRange(column, row_num);
            size_t appended = 0;
            while (!cursor.finished && appended < max_batch_points)
            {
                const auto outer = getPolygonOuterRange(polygons.data, cursor.component_index);
                chassert(outer.has_value());
                appended += appendPointRangeBatch(*outer, cursor.point_index, max_batch_points - appended, out);
                if (cursor.point_index == outer->end)
                {
                    ++cursor.component_index;
                    advanceToNextNonEmptyPolygonOuter(polygons, cursor);
                }
            }
            return appended;
        }
        case GeometryColumnType::Null:
            return 0;
    }

    UNREACHABLE();
}

}
