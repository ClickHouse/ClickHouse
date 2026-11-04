#include <Functions/FunctionFactory.h>
#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <Columns/ColumnVariant.h>

#include <base/EnumReflection.h>

#include <boost/geometry/algorithms/envelope.hpp>
#include <boost/geometry/geometries/box.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Traits>
class FunctionBoundingBox : public IFunction
{
public:
    using Point = typename Traits::PointType;
    static constexpr auto name = Traits::name;

    explicit FunctionBoundingBox() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBoundingBox>(); }

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

        return DataTypeFactory::instance().get("Ring");
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return DataTypeFactory::instance().get("Ring");
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Geometry functions work with the Geometry type directly which is a Variant with custom name,
    /// and not with individual variant alternatives. So, don't use default implementation.
    bool useDefaultImplementationForVariant() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        RingSerializer<Point> serializer;

        const auto * column_variant = typeid_cast<const ColumnVariant *>(arguments.front().column.get());
        if (column_variant)
        {
            Field field;
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
        else
        {
            callOnGeometryDataType<Point>(
                arguments[0].type,
                [&](const auto & type)
                {
                    using TypeConverter = std::decay_t<decltype(type)>;
                    using Converter = typename TypeConverter::Type;

                    auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        Ring<Point> ring = computeEnvelope(geometries[i]);
                        serializer.add(ring);
                    }
                });
        }

        return serializer.finalize();
    }

private:
    /// Convert a boost::geometry::model::box to a Ring (5-point closed rectangle).
    /// Note: For spherical coordinates crossing the date line, Boost may return
    /// longitude values > 180 (e.g., 181 represents -179). This is intentional
    /// and mathematically correct - normalizing to [-180, 180] would produce a
    /// ring spanning ~358° instead of the actual ~2°.
    static Ring<Point> boxToRing(const boost::geometry::model::box<Point> & box)
    {
        auto min_x = box.min_corner().template get<0>();
        auto min_y = box.min_corner().template get<1>();
        auto max_x = box.max_corner().template get<0>();
        auto max_y = box.max_corner().template get<1>();

        Ring<Point> ring;
        ring.reserve(5);
        ring.emplace_back(min_x, min_y);
        ring.emplace_back(max_x, min_y);
        ring.emplace_back(max_x, max_y);
        ring.emplace_back(min_x, max_y);
        ring.emplace_back(min_x, min_y);
        return ring;
    }

    template <typename Geom>
    static Ring<Point> computeEnvelope(Geom & geom)
    {
        if constexpr (std::is_same_v<Point, SphericalPoint>)
        {
            checkSphericalLatitudes(geom, name);
            /// On a sphere, the winding order decides which side of a ring is the interior: a ring around a pole wound
            /// the other way encloses the rest of the sphere. `boost::geometry::correct` fixes the orientation and
            /// closes the rings; points and linestrings are left as is.
            boost::geometry::correct(geom);
        }

        boost::geometry::model::box<Point> box{};
        boost::geometry::envelope(geom, box);
        /// An empty geometry has no bounding box: `boost::geometry::envelope` leaves the box inverted.
        if (box.min_corner().template get<0>() > box.max_corner().template get<0>())
            return {};
        return boxToRing(box);
    }

    static void processField(const Field & field, GeometryColumnType type, RingSerializer<Point> & serializer)
    {
        auto add = [&](auto geometry)
        {
            checkPointsAreFinite(geometry);
            serializer.add(computeEnvelope(geometry));
        };

        switch (type)
        {
            case GeometryColumnType::Point:
                add(getPointFromField<Point>(field));
                break;
            case GeometryColumnType::Linestring:
                add(getLineStringFromField<Point>(field));
                break;
            case GeometryColumnType::Ring:
                add(getRingFromField<Point>(field));
                break;
            case GeometryColumnType::Polygon:
                add(getPolygonFromField<Point>(field));
                break;
            case GeometryColumnType::MultiLinestring:
                add(getMultiLineStringFromField<Point>(field));
                break;
            case GeometryColumnType::MultiPolygon:
                add(getMultiPolygonFromField<Point>(field));
                break;
            case GeometryColumnType::MultiPoint:
                add(getMultiPointFromField<Point>(field));
                break;
            case GeometryColumnType::Null:
                serializer.add(Ring<Point>{});
                break;
        }
    }
};

struct CartesianTraits
{
    using PointType = CartesianPoint;
    static constexpr auto name = "boundingBoxCartesian";
};

struct SphericalTraits
{
    using PointType = SphericalPoint;
    static constexpr auto name = "boundingBoxSpherical";
};

}

REGISTER_FUNCTION(BoundingBox)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates the axis-aligned bounding box of a geometry in Cartesian coordinates.
Returns a Ring (closed rectangle with sides parallel to the coordinate axes).
For an empty geometry, the result is an empty Ring `[]`.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "boundingBoxCartesian(geometry)";
    FunctionDocumentation::Arguments arguments_cartesian
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point", "MultiPoint"}}};
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the bounding box as a Ring (closed rectangle).", {"Ring"}};
    FunctionDocumentation::Examples examples_cartesian
        = {{"Bounding box of a triangle",
            R"(
SELECT boundingBoxCartesian([[(0., 0.), (3., 5.), (6., 0.), (0., 0.)]])
        )",
            R"(
[(0,0),(6,0),(6,5),(0,5),(0,0)]
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {26, 11};
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

    factory.registerFunction<FunctionBoundingBox<CartesianTraits>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(
Calculates the bounding box of a geometry in spherical coordinates.
Returns a Ring (closed rectangle). For geometries crossing the date line (±180°),
the returned longitude values may exceed 180° (e.g., 181 represents -179°).
This is the correct mathematical representation; normalizing to [-180, 180] would
produce an incorrect box spanning ~358° instead of ~2°.

The winding order of the rings does not matter: for example, the box of a ring around a pole includes the pole.
For an empty geometry, the result is an empty Ring `[]`.
A latitude outside [−90, 90] throws an exception.
    )";
    FunctionDocumentation::Syntax syntax_spherical = "boundingBoxSpherical(geometry)";
    FunctionDocumentation::Arguments arguments_spherical
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point", "MultiPoint"}}};
    FunctionDocumentation::ReturnedValue returned_value_spherical
        = {"Returns the bounding box as a Ring (closed rectangle) in spherical coordinates.", {"Ring"}};
    FunctionDocumentation::Examples examples_spherical
        = {{"Bounding box of a polygon in spherical coordinates",
            R"(
SELECT boundingBoxSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]])
        )",
            R"(
[(4.338074,50.833264),(4.367945,50.833264),(4.367945,50.858306),(4.338074,50.858306),(4.338074,50.833264)]
        )"},
           {"Bounding box crossing the date line (note longitude > 180°)",
            R"(
SELECT boundingBoxSpherical(CAST([(179., 50.), (-179., 50.)], 'LineString'))
        )",
            R"(
[(179,50),(181,50),(181,50.004297195777966),(179,50.004297195777966),(179,50)]
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {26, 11};
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

    factory.registerFunction<FunctionBoundingBox<SphericalTraits>>(function_documentation_spherical);
}

}
