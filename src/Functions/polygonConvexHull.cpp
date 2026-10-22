#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>


#include <algorithm>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// `boost::geometry::convex_hull` takes only the exterior rings of polygons into account,
/// but it checks for emptiness with `boost::geometry::is_empty`, which also looks at the interior rings.
/// For a polygon with an empty exterior ring and a non-empty interior ring it reads uninitialized points.
/// Returns true if there are no points for the convex hull to be built from.
template <typename Geometry>
bool hasNoPointsForConvexHull(const Geometry & geometry)
{
    if constexpr (std::is_same_v<boost::geometry::tag_t<Geometry>, boost::geometry::polygon_tag>)
        return geometry.outer().empty();
    else if constexpr (std::is_same_v<boost::geometry::tag_t<Geometry>, boost::geometry::multi_polygon_tag>)
        return std::ranges::all_of(geometry, [](const auto & polygon) { return polygon.outer().empty(); });
    else
        return boost::geometry::is_empty(geometry);
}

template <typename Point>
class FunctionPolygonConvexHull final : public IFunction
{
public:
    static const char * name;

    explicit FunctionPolygonConvexHull() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonConvexHull>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    String getSignatureString() const override
    {
        return "(Any) -> Polygon";
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        PolygonSerializer<Point> serializer;

        callOnGeometryDataType<Point>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            if constexpr (std::is_same_v<Converter, ColumnToPointsConverter<Point>>)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of function {} must not be a Point", getName());
            else
            {
                auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    Polygon<Point> convex_hull{};
                    if (!hasNoPointsForConvexHull(geometries[i]))
                        boost::geometry::convex_hull(geometries[i], convex_hull);
                    serializer.add(convex_hull);
                }
            }
        }
        );

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonConvexHull<CartesianPoint>::name = "polygonConvexHullCartesian";

}

REGISTER_FUNCTION(PolygonConvexHull)
{
    FunctionDocumentation::Description description = R"(
Calculates a convex hull. [Reference](https://www.boost.org/doc/libs/1_61_0/libs/geometry/doc/html/geometry/reference/algorithms/convex_hull.html)

Coordinates are in Cartesian coordinate system.
    )";
    FunctionDocumentation::Syntax syntax = "polygonConvexHullCartesian(multipolygon)";
    FunctionDocumentation::Arguments arguments = {
        {"multipolygon", "A MultiPolygon value.", {"MultiPolygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the convex hull as a Polygon.", {"Polygon"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Conve hull example",
        R"(
SELECT wkt(polygonConvexHullCartesian([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.), (2., 3.)]]]))
        )",
        R"(
POLYGON((0 0,0 5,5 5,5 0,0 0))
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPolygonConvexHull<CartesianPoint>>(function_documentation);
}

}
