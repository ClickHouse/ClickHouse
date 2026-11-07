#pragma once

#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <base/EnumReflection.h>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Field.h>
#include <base/types.h>
#include <DataTypes/DataTypeVariant.h>
#include <Functions/FunctionHelpers.h>

#include <cfloat>
#include <cmath>
#include <memory>
#include <numbers>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Point>
Point getPointFromField(const Field & field)
{
    const auto & point = field.safeGet<Tuple>();
    auto x = point.at(0).safeGet<Float64>();
    auto y = point.at(1).safeGet<Float64>();
    return {x, y};
}

template <typename Point>
MultiPoint<Point> getMultiPointFromField(const Field & field)
{
    MultiPoint<Point> multipoint;
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        multipoint.push_back(getPointFromField<Point>(tuple));
    return multipoint;
}

template <typename Point>
LineString<Point> getLineStringFromField(const Field & field)
{
    LineString<Point> linestring;
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        linestring.push_back(getPointFromField<Point>(tuple));
    return linestring;
}

template <typename Point>
Ring<Point> getRingFromField(const Field & field)
{
    Ring<Point> ring;
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        ring.push_back(getPointFromField<Point>(tuple));
    return ring;
}

template <typename Point>
MultiLineString<Point> getMultiLineStringFromField(const Field & field)
{
    MultiLineString<Point> multilinestring;
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        multilinestring.push_back(getLineStringFromField<Point>(tuple));
    return multilinestring;
}

template <typename Point>
Polygon<Point> getPolygonFromField(const Field & field)
{
    Polygon<Point> polygon;
    const auto & array = field.safeGet<Array>();
    for (size_t i = 0; i < array.size(); ++i)
    {
        const auto & ring = array.at(i);
        if (i == 0)
            polygon.outer() = getRingFromField<Point>(ring);
        else
            polygon.inners().push_back(getRingFromField<Point>(ring));
    }
    return polygon;
}

template <typename Point>
MultiPolygon<Point> getMultiPolygonFromField(const Field & field)
{
    MultiPolygon<Point> polygon;
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        polygon.push_back(getPolygonFromField<Point>(tuple));
    return polygon;
}

/// Throws if a coordinate in `geometry` is NaN or infinite. The column converters check this, but the `Field` getters
/// above do not.
template <typename Geometry>
void checkPointsAreFinite(const Geometry & geometry)
{
    boost::geometry::for_each_point(geometry, [](const auto & point)
    {
        const Float64 first = boost::geometry::get<0>(point);
        const Float64 second = boost::geometry::get<1>(point);
        if (isNaN(first) || isNaN(second))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Point's component must not be NaN");
        if (std::isinf(first) || std::isinf(second))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Point's component must not be infinite");
    });
}

/// Throws if a latitude in `geometry` is outside [−90, 90] degrees. Longitudes wrap around, so any finite one is valid.
template <typename Geometry>
void checkSphericalLatitudes(const Geometry & geometry, std::string_view function_name)
{
    boost::geometry::for_each_point(geometry, [function_name](const SphericalPoint & point)
    {
        const Float64 latitude = boost::geometry::get<1>(point);
        if (!(latitude >= -90 && latitude <= 90))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Point ({}, {}) is invalid in function {}. For a valid point, the latitude is between -90 and 90 degrees inclusive",
                boost::geometry::get<0>(point),
                latitude,
                function_name);
    });
}

/// The number of vertices of a ring, not counting a closing vertex equal to the first one.
template <typename Point>
size_t ringVertexCount(const Ring<Point> & ring)
{
    size_t n = ring.size();
    if (n >= 2 && boost::geometry::get<0>(ring.front()) == boost::geometry::get<0>(ring.back())
        && boost::geometry::get<1>(ring.front()) == boost::geometry::get<1>(ring.back()))
        --n;
    return n;
}

/// The number of edges between two different vertices among the first `n` vertices of a ring, including the edge from
/// the last vertex to the first one. An edge between equal vertices adds nothing to the area of the ring, nor to the
/// rounding error of the area.
template <typename Point>
size_t ringEdgeCount(const Ring<Point> & ring, size_t n)
{
    size_t edges = 0;
    for (size_t i = 0; i < n; ++i)
    {
        const Point & from = ring[i];
        const Point & to = ring[i + 1 == n ? 0 : i + 1];
        if (boost::geometry::get<0>(from) != boost::geometry::get<0>(to) || boost::geometry::get<1>(from) != boost::geometry::get<1>(to))
            ++edges;
    }
    return edges;
}

/// Whether `a` and `b` are antipodal. Both latitudes must be in [−90, 90].
inline bool areAntipodal(const SphericalPoint & a, const SphericalPoint & b)
{
    const Float64 latitude = boost::geometry::get<1>(a);
    if (latitude != -boost::geometry::get<1>(b))
        return false;
    if (std::abs(latitude) == 90)
        return true;
    /// Each longitude is reduced first, so that the difference of two large ones does not overflow.
    const Float64 longitude_difference
        = std::remainder(boost::geometry::get<0>(a), 360.0) - std::remainder(boost::geometry::get<0>(b), 360.0);
    return std::abs(std::remainder(longitude_difference, 360.0)) == 180;
}

/// Throws if two consecutive vertices of `points` are antipodal. If `closed`, the last vertex and the first one are
/// consecutive too.
template <typename Points>
void checkSphericalPathEdges(const Points & points, bool closed, std::string_view function_name)
{
    const size_t n = points.size();
    const size_t edges = closed ? n : (n == 0 ? 0 : n - 1);
    for (size_t i = 0; i < edges; ++i)
    {
        const SphericalPoint & from = points[i];
        const SphericalPoint & to = points[i + 1 == n ? 0 : i + 1];
        if (areAntipodal(from, to))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The edge from ({}, {}) to ({}, {}) in function {} joins antipodal points, so its path is undefined: every half "
                "of a great circle through them is a shortest path. Add a vertex between them",
                boost::geometry::get<0>(from),
                boost::geometry::get<1>(from),
                boost::geometry::get<0>(to),
                boost::geometry::get<1>(to),
                function_name);
    }
}

/// Throws if an edge of a ring or a linestring in `geometry` joins two antipodal points. Otherwise the path of the edge
/// would depend on the rounding errors of the coordinates.
template <typename Geometry>
void checkSphericalEdges(const Geometry & geometry, std::string_view function_name)
{
    using Tag = typename boost::geometry::tag<Geometry>::type;
    if constexpr (std::is_same_v<Tag, boost::geometry::linestring_tag>)
    {
        checkSphericalPathEdges(geometry, /* closed */ false, function_name);
    }
    else if constexpr (std::is_same_v<Tag, boost::geometry::ring_tag>)
    {
        checkSphericalPathEdges(geometry, /* closed */ true, function_name);
    }
    else if constexpr (std::is_same_v<Tag, boost::geometry::polygon_tag>)
    {
        checkSphericalEdges(geometry.outer(), function_name);
        for (const auto & inner : geometry.inners())
            checkSphericalEdges(inner, function_name);
    }
    else if constexpr (std::is_same_v<Tag, boost::geometry::multi_linestring_tag> || std::is_same_v<Tag, boost::geometry::multi_polygon_tag>)
    {
        for (const auto & part : geometry)
            checkSphericalEdges(part, function_name);
    }
    else
    {
        static_assert(std::is_same_v<Tag, boost::geometry::point_tag> || std::is_same_v<Tag, boost::geometry::multi_point_tag>);
    }
}

/// Whether a ring of `edges` edges (`ringEdgeCount`) whose area, of either of the two regions it bounds, is `area`
/// steradians splits the sphere into two equal halves up to rounding, as the equator does. Then the interior of the ring
/// is the half on its right, as for the clockwise `Ring`. Otherwise it is the smaller region. The bound of the rounding
/// error is the one of `S2::GetCurvatureMaxError`, but without the edges of zero length, so that repeating a vertex does
/// not change the interior.
inline bool isHemisphere(Float64 area, size_t edges)
{
    return std::abs(2 * std::numbers::pi - std::abs(area)) <= 11.25 * DBL_EPSILON * static_cast<Float64>(edges);
}

/// Must match the global discriminators of the Geometry Variant type.
enum class GeometryColumnType
{
    Linestring = 0,
    MultiLinestring = 1,
    MultiPolygon = 2,
    Point = 3,
    Polygon = 4,
    Ring = 5,
    MultiPoint = 6,
    Null = 255
};

/// Tries to determine the geometry column type from a DataType.
/// Works for both custom-named types (like "Polygon") and base types
/// (like "Array(Array(Tuple(Float64, Float64)))") when the custom name is lost.
/// Returns std::nullopt if the type is not a recognized geometry type.
inline std::optional<GeometryColumnType> getGeometryColumnTypeFromDataType(const DataTypePtr & type)
{
    const String & type_name = type->getName();

    /// Check custom type names first.
    if (type_name == "Point") return GeometryColumnType::Point;
    if (type_name == "MultiPoint") return GeometryColumnType::MultiPoint;
    if (type_name == "Ring") return GeometryColumnType::Ring;
    if (type_name == "LineString") return GeometryColumnType::Linestring;
    if (type_name == "Polygon") return GeometryColumnType::Polygon;
    if (type_name == "MultiLineString") return GeometryColumnType::MultiLinestring;
    if (type_name == "MultiPolygon") return GeometryColumnType::MultiPolygon;

    /// Check structural types (when custom names are lost during expression analysis).
    auto is_point_type = [](const DataTypePtr & dt) -> bool
    {
        const auto * tuple = typeid_cast<const DataTypeTuple *>(dt.get());
        if (!tuple)
            return false;
        const auto & elems = tuple->getElements();
        return elems.size() == 2
            && WhichDataType(elems[0]).isFloat64()
            && WhichDataType(elems[1]).isFloat64();
    };

    /// Point: Tuple(Float64, Float64)
    if (is_point_type(type))
        return GeometryColumnType::Point;

    /// Ring/LineString: Array(Tuple(Float64, Float64))
    const auto * array1 = typeid_cast<const DataTypeArray *>(type.get());
    if (!array1)
        return std::nullopt;

    if (is_point_type(array1->getNestedType()))
        return GeometryColumnType::Ring;

    /// Polygon/MultiLineString: Array(Array(Tuple(Float64, Float64)))
    const auto * array2 = typeid_cast<const DataTypeArray *>(array1->getNestedType().get());
    if (!array2)
        return std::nullopt;

    if (is_point_type(array2->getNestedType()))
        return GeometryColumnType::Polygon;

    /// MultiPolygon: Array(Array(Array(Tuple(Float64, Float64))))
    const auto * array3 = typeid_cast<const DataTypeArray *>(array2->getNestedType().get());
    if (!array3)
        return std::nullopt;

    if (is_point_type(array3->getNestedType()))
        return GeometryColumnType::MultiPolygon;

    return std::nullopt;
}

}

/// GeometryColumnType has Null = 255, which is outside the default magic_enum range [-128, 127].
template <> struct magic_enum::customize::enum_range<DB::GeometryColumnType>
{
    static constexpr int min = 0;
    static constexpr int max = 255;
};

namespace DB
{

template <typename Point, typename FunctionToCalculate>
class FunctionGeometry final : public IFunction
{
public:
    static const char * name;

    explicit FunctionGeometry() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGeometry>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    /// Declarative signature — covers `areaCartesian`, `areaSpherical`,
    /// `perimeterCartesian`, `perimeterSpherical`: a single geometry-typed
    /// argument, a `Float64` result.
    String getSignatureString() const override
    {
        return "(Any) -> Float64";
    }

    /// The `(Any)` signature is documentation-only: the DSL cannot express "must be a Geometry
    /// (or a custom Variant) type", so keep the legacy `getReturnTypeImpl(DataTypes)` authoritative
    /// to reject non-geometry arguments at analysis time instead of failing later in execution.
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;
        return getReturnTypeImpl(data_types);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->getName() != "Geometry" && !getGeometryColumnTypeFromDataType(arguments[0]))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} should be Geometry, got {}", getName(), arguments[0]->getName());
        }

        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Geometry functions work with the Geometry type directly which is a Variant with custom name,
    /// and not with individual variant alternatives. So, don't use default implementation.
    bool useDefaultImplementationForVariant() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnFloat64::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        const auto * column_variant = typeid_cast<const ColumnVariant *>(arguments.front().column.get());
        if (column_variant)
        {
            /// Geometry (Variant) type path.
            /// GeometryColumnType matches the global discriminators of the Geometry Variant, while the local
            /// order can differ (e.g. after a variant-to-variant conversion), so map local to global per row.
            Field field;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                column_variant->get(i, field);
                auto global_discr = column_variant->globalDiscriminatorAt(i);
                auto type = magic_enum::enum_cast<GeometryColumnType>(global_discr);
                if (!type)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of geometry {}", static_cast<Int32>(global_discr));
                processField(field, *type, res_data);
            }
        }
        else
        {
            /// Individual geometry type path (e.g. Polygon from readWKTPolygon).
            auto geo_type = getGeometryColumnTypeFromDataType(arguments.front().type);
            if (!geo_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument of function {} should be Geometry, got {}", getName(), arguments.front().type->getName());

            Field field;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                arguments.front().column->get(i, field);
                processField(field, *geo_type, res_data);
            }
        }

        return res_column;
    }

private:
    void processField(const Field & field, GeometryColumnType type, PaddedPODArray<Float64> & res_data) const
    {
        switch (type)
        {
            case GeometryColumnType::Linestring:
            {
                LineString<Point> linestring = getLineStringFromField<Point>(field);
                res_data.push_back(FunctionToCalculate()(linestring));
                break;
            }
            case GeometryColumnType::MultiLinestring:
            {
                MultiLineString<Point> multilinestring = getMultiLineStringFromField<Point>(field);
                res_data.push_back(FunctionToCalculate()(multilinestring));
                break;
            }
            case GeometryColumnType::MultiPoint:
            {
                MultiPoint<Point> multipoint = getMultiPointFromField<Point>(field);
                res_data.push_back(FunctionToCalculate()(multipoint));
                break;
            }
            case GeometryColumnType::MultiPolygon:
            {
                MultiPolygon<Point> multipolygon = getMultiPolygonFromField<Point>(field);
                res_data.push_back(FunctionToCalculate()(multipolygon));
                break;
            }
            case GeometryColumnType::Point:
            {
                Point point = getPointFromField<Point>(field);
                res_data.push_back(FunctionToCalculate()(point));
                break;
            }
            case GeometryColumnType::Polygon:
            {
                Polygon<Point> polygon = getPolygonFromField<Point>(field);
                res_data.push_back(FunctionToCalculate()(polygon));
                break;
            }
            case GeometryColumnType::Ring:
            {
                Ring<Point> ring = getRingFromField<Point>(field);
                res_data.push_back(FunctionToCalculate()(ring));
                break;
            }
            case GeometryColumnType::Null:
            {
                res_data.push_back(0);
                break;
            }
        }
    }

public:

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

}
