#include <Functions/FunctionFactory.h>
#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <Columns/ColumnVariant.h>

#include <base/EnumReflection.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wambiguous-reversed-operator"
#include <s2/s2latlng.h>
#include <s2/s2loop_measures.h>
#include <s2/s2point.h>
#include <s2/s2point_span.h>
#include <s2/s2polyline_measures.h>
#pragma clang diagnostic pop

#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Convert a range of SphericalPoints (lon, lat in degrees) to S2Points.
/// Reuses the caller-owned buffer to avoid per-call heap allocations.
template <typename Container>
void toS2Points(const Container & vertices, std::vector<S2Point> & out)
{
    out.clear();
    out.reserve(vertices.size());
    for (const auto & p : vertices)
        out.push_back(S2LatLng::FromDegrees(p.template get<1>(), p.template get<0>()).ToPoint());
}

/// Convert an S2Point back to SphericalPoint (lon, lat in degrees).
SphericalPoint toSphericalPoint(const S2Point & p)
{
    S2LatLng ll(p);
    return SphericalPoint{ll.lng().degrees(), ll.lat().degrees()};
}

/// Norm2 below this means the S2Point is degenerate (cancellation or zero-area input).
constexpr double S2_ZERO_NORM2_THRESHOLD = 1e-60;

/// Normalize an S2Point centroid and convert to SphericalPoint, with fallback for degenerate inputs.
SphericalPoint normalizeAndProject(const S2Point & centroid, const SphericalPoint & fallback)
{
    if (centroid.Norm2() < S2_ZERO_NORM2_THRESHOLD)
        return fallback;
    return toSphericalPoint(centroid.Normalize());
}

/// Strip closing vertex from an S2Point loop if the ring repeats its first vertex.
/// S2 loops are implicitly closed, so the duplicate must be removed.
size_t loopVertexCount(const Ring<SphericalPoint> & ring)
{
    size_t n = ring.size();
    if (n >= 2 && ring.front().template get<0>() == ring.back().template get<0>()
        && ring.front().template get<1>() == ring.back().template get<1>())
        --n;
    return n;
}

/// Compute the true centroid × area for a ring using S2::GetCentroid,
/// with sign normalized to positive (CCW) orientation so that
/// polygon hole subtraction (outer - holes) works correctly.
S2Point computeRingCentroidS2(const Ring<SphericalPoint> & ring, std::vector<S2Point> & scratch)
{
    size_t n = loopVertexCount(ring);
    if (n < 3)
        return S2Point(0, 0, 0);

    toS2Points(ring, scratch);

    S2PointLoopSpan loop(scratch.data(), n);
    S2Point centroid = S2::GetCentroid(loop);

    /// S2::GetCentroid returns centroid × signed_area (positive for CCW).
    /// We always want positive orientation so that the caller can do
    /// explicit outer + (−holes). Flip if the ring was CW (negative area).
    if (S2::GetSignedArea(loop) < 0)
        centroid = S2Point(-centroid.x(), -centroid.y(), -centroid.z());

    return centroid;
}

SphericalPoint computeRingSphericalCentroid(const Ring<SphericalPoint> & ring, std::vector<S2Point> & scratch)
{
    S2Point centroid = computeRingCentroidS2(ring, scratch);
    return normalizeAndProject(centroid, ring.empty() ? SphericalPoint{0, 0} : ring.front());
}

SphericalPoint computePolygonSphericalCentroid(const Polygon<SphericalPoint> & polygon, std::vector<S2Point> & scratch)
{
    S2Point centroid(0, 0, 0);

    const auto & outer = polygon.outer();
    if (!outer.empty())
        centroid += computeRingCentroidS2(outer, scratch);

    for (const auto & inner : polygon.inners())
    {
        if (!inner.empty())
            centroid -= computeRingCentroidS2(inner, scratch);
    }

    SphericalPoint fallback = polygon.outer().empty() ? SphericalPoint{0, 0} : polygon.outer().front();
    return normalizeAndProject(centroid, fallback);
}

SphericalPoint computeMultiPolygonSphericalCentroid(const MultiPolygon<SphericalPoint> & multipolygon, std::vector<S2Point> & scratch)
{
    S2Point centroid(0, 0, 0);

    for (const auto & polygon : multipolygon)
    {
        centroid += computeRingCentroidS2(polygon.outer(), scratch);
        for (const auto & inner : polygon.inners())
            centroid -= computeRingCentroidS2(inner, scratch);
    }

    return normalizeAndProject(centroid, SphericalPoint{0, 0});
}

SphericalPoint computeLineStringSphericalCentroid(const LineString<SphericalPoint> & linestring, std::vector<S2Point> & scratch)
{
    if (linestring.size() < 2)
        return linestring.empty() ? SphericalPoint{0, 0} : linestring.front();

    toS2Points(linestring, scratch);
    S2Point centroid = S2::GetCentroid(S2PointSpan(scratch));

    return normalizeAndProject(centroid, SphericalPoint{0, 0});
}

SphericalPoint computeMultiLineStringSphericalCentroid(const MultiLineString<SphericalPoint> & multilinestring, std::vector<S2Point> & scratch)
{
    S2Point centroid(0, 0, 0);

    for (const auto & linestring : multilinestring)
    {
        if (linestring.size() < 2)
            continue;
        toS2Points(linestring, scratch);
        centroid += S2::GetCentroid(S2PointSpan(scratch));
    }

    return normalizeAndProject(centroid, SphericalPoint{0, 0});
}

/// Dispatch to the appropriate centroid function based on geometry type.
template <typename Geometry>
SphericalPoint computeGeometrySphericalCentroid(const Geometry & geom, std::vector<S2Point> & scratch)
{
    if constexpr (std::is_same_v<Geometry, Ring<SphericalPoint>>)
        return computeRingSphericalCentroid(geom, scratch);
    else if constexpr (std::is_same_v<Geometry, Polygon<SphericalPoint>>)
        return computePolygonSphericalCentroid(geom, scratch);
    else if constexpr (std::is_same_v<Geometry, MultiPolygon<SphericalPoint>>)
        return computeMultiPolygonSphericalCentroid(geom, scratch);
    else if constexpr (std::is_same_v<Geometry, LineString<SphericalPoint>>)
        return computeLineStringSphericalCentroid(geom, scratch);
    else if constexpr (std::is_same_v<Geometry, MultiLineString<SphericalPoint>>)
        return computeMultiLineStringSphericalCentroid(geom, scratch);
    else
    {
        static_assert(std::is_same_v<Geometry, SphericalPoint>);
        return geom;
    }
}

template <typename Traits>
class FunctionCentroid : public IFunction
{
public:
    using Point = typename Traits::PointType;
    static constexpr auto name = Traits::name;

    explicit FunctionCentroid() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCentroid>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->getName() != "Geometry" && !getGeometryColumnTypeFromDataType(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} should be a geometry type, got {}",
                getName(),
                arguments[0]->getName());

        return DataTypeFactory::instance().get("Point");
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return DataTypeFactory::instance().get("Point");
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForVariant() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        PointSerializer<Point> serializer;

        const auto * column_variant = typeid_cast<const ColumnVariant *>(arguments.front().column.get());
        if (column_variant)
        {
            Field field;
            if constexpr (std::is_same_v<Point, SphericalPoint>)
            {
                std::vector<S2Point> scratch;
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    column_variant->get(i, field);
                    auto global_discr = column_variant->globalDiscriminatorAt(i);
                    auto type = magic_enum::enum_cast<GeometryColumnType>(global_discr);
                    if (!type)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of geometry {}", static_cast<Int32>(global_discr));
                    processField(field, *type, serializer, scratch);
                }
            }
            else
            {
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    column_variant->get(i, field);
                    auto global_discr = column_variant->globalDiscriminatorAt(i);
                    auto type = magic_enum::enum_cast<GeometryColumnType>(global_discr);
                    if (!type)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of geometry {}", static_cast<Int32>(global_discr));
                    processField(field, *type, serializer);
                }
            }
        }
        else
        {
            callOnGeometryDataType<Point>(
                arguments[0].type,
                [&](const auto & type)
                {
                    using TypeConverter = std::decay_t<decltype(type)>;
                    using Converter = typename TypeConverter::Type;

                    auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                    if constexpr (std::is_same_v<Point, SphericalPoint>)
                    {
                        std::vector<S2Point> scratch;
                        for (size_t i = 0; i < input_rows_count; ++i)
                        {
                            Point centroid = computeGeometrySphericalCentroid(geometries[i], scratch);
                            serializer.add(centroid);
                        }
                    }
                    else
                    {
                        for (size_t i = 0; i < input_rows_count; ++i)
                            serializer.add(computeCartesianCentroid(geometries[i]));
                    }
                });
        }

        return serializer.finalize();
    }

private:
    /// Compute Cartesian centroid with fallback for degenerate geometries
    /// (empty, zero-area) where boost::geometry::centroid throws centroid_exception.
    template <typename Geom>
    static Point computeCartesianCentroid(const Geom & geom)
    {
        Point centroid{};
        try
        {
            boost::geometry::centroid(geom, centroid);
        }
        catch (const boost::geometry::centroid_exception &)
        {
            centroid = Point{0, 0};
        }
        return centroid;
    }

    template <typename CentroidFn>
    static void processFieldImpl(const Field & field, GeometryColumnType type, PointSerializer<Point> & serializer, CentroidFn && centroid)
    {
        switch (type)
        {
            case GeometryColumnType::Point:
                serializer.add(getPointFromField<Point>(field));
                break;
            case GeometryColumnType::Linestring:
                serializer.add(centroid(getLineStringFromField<Point>(field)));
                break;
            case GeometryColumnType::Ring:
                serializer.add(centroid(getRingFromField<Point>(field)));
                break;
            case GeometryColumnType::Polygon:
                serializer.add(centroid(getPolygonFromField<Point>(field)));
                break;
            case GeometryColumnType::MultiLinestring:
                serializer.add(centroid(getMultiLineStringFromField<Point>(field)));
                break;
            case GeometryColumnType::MultiPolygon:
                serializer.add(centroid(getMultiPolygonFromField<Point>(field)));
                break;
            case GeometryColumnType::Null:
                serializer.add(Point{0, 0});
                break;
        }
    }

    static void processField(const Field & field, GeometryColumnType type, PointSerializer<Point> & serializer)
    {
        static_assert(std::is_same_v<Point, CartesianPoint>);
        processFieldImpl(field, type, serializer, [](auto && geom) { return computeCartesianCentroid(geom); });
    }

    static void
    processField(const Field & field, GeometryColumnType type, PointSerializer<Point> & serializer, std::vector<S2Point> & scratch)
    {
        static_assert(std::is_same_v<Point, SphericalPoint>);
        processFieldImpl(
            field, type, serializer, [&scratch](auto && geom) -> Point { return computeGeometrySphericalCentroid(geom, scratch); });
    }
};

struct CartesianTraits
{
    using PointType = CartesianPoint;
    static constexpr auto name = "centroidCartesian";
};

struct SphericalTraits
{
    using PointType = SphericalPoint;
    static constexpr auto name = "centroidSpherical";
};

}

REGISTER_FUNCTION(Centroid)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates the centroid (center of mass) of a geometry in Cartesian coordinates.
Takes any geometry type and returns a Point.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "centroidCartesian(geometry)";
    FunctionDocumentation::Arguments arguments_cartesian
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point"}}};
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the centroid of the geometry as a Point.", {"Point"}};
    FunctionDocumentation::Examples examples_cartesian
        = {{"Centroid of a square",
            R"(
SELECT centroidCartesian([[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]])
        )",
            R"(
(2,2)
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {26, 5};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian
        = {description_cartesian,
           syntax_cartesian,
           arguments_cartesian,
           {},
           returned_value_cartesian,
           examples_cartesian,
           introduced_in_cartesian,
           category_cartesian};

    factory.registerFunction<FunctionCentroid<CartesianTraits>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(
Calculates the centroid (center of mass) of a geometry in spherical coordinates.
Takes any geometry type and returns a Point. Uses the true centroid (mass centroid)
which correctly accounts for the curvature of the sphere.
    )";
    FunctionDocumentation::Syntax syntax_spherical = "centroidSpherical(geometry)";
    FunctionDocumentation::Arguments arguments_spherical
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point"}}};
    FunctionDocumentation::ReturnedValue returned_value_spherical
        = {"Returns the centroid of the geometry as a Point in spherical coordinates.", {"Point"}};
    FunctionDocumentation::Examples examples_spherical
        = {{"Centroid of a polygon in spherical coordinates",
            R"(
SELECT centroidSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]])
        )",
            R"(
(4.352733722119422,50.846349225815466)
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {26, 5};
    FunctionDocumentation::Category category_spherical = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_spherical
        = {description_spherical,
           syntax_spherical,
           arguments_spherical,
           {},
           returned_value_spherical,
           examples_spherical,
           introduced_in_spherical,
           category_spherical};

    factory.registerFunction<FunctionCentroid<SphericalTraits>>(function_documentation_spherical);
}

}
