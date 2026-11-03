#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Common/logger_useful.h>

#include <Columns/ColumnTuple.h>

#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Point>
class FunctionPolygonsUnion : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonsUnion() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonsUnion>();
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
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return DataTypeFactory::instance().get("MultiPolygon");
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        MultiPolygonSerializer<Point> serializer;

        callOnTwoGeometryDataTypes<Point>(arguments[0].type, arguments[1].type, [&](const auto & left_type, const auto & right_type)
        {
            using LeftConverterType = std::decay_t<decltype(left_type)>;
            using RightConverterType = std::decay_t<decltype(right_type)>;

            using LeftConverter = typename LeftConverterType::Type;
            using RightConverter = typename RightConverterType::Type;

            if constexpr (std::is_same_v<ColumnToPointsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToPointsConverter<Point>, RightConverter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be Point", getName());
            else if constexpr (std::is_same_v<ColumnToLineStringsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToLineStringsConverter<Point>, RightConverter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be LineString", getName());
            else if constexpr (std::is_same_v<ColumnToMultiLineStringsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToMultiLineStringsConverter<Point>, RightConverter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be MultiLineString", getName());
            else
            {
                auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                /// We are not interested in some pitfalls in third-party libraries
                /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    /// Orient the polygons correctly.
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    MultiPolygon<Point> polygons_union{};
                    /// Main work here.
                    boost::geometry::union_(first[i], second[i], polygons_union);

                    serializer.add(polygons_union);
                }
            }
        });

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonsUnion<CartesianPoint>::name = "polygonsUnionCartesian";

template <>
const char * FunctionPolygonsUnion<SphericalPoint>::name = "polygonsUnionSpherical";

}

REGISTER_FUNCTION(PolygonsUnion)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates the union of polygons.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "polygonsUnionCartesian(polygon1, polygon2)";
    FunctionDocumentation::Arguments arguments_cartesian = {
        {"polygon1", "The first Polygon.", {"Polygon"}},
        {"polygon2", "The second Polygon.", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the union of the polygons as a MultiPolygon.", {"MultiPolygon"}};
    FunctionDocumentation::Examples examples_cartesian =
    {
    {
        "Usage example",
        R"(
SELECT wkt(polygonsUnionCartesian([[[(0., 0.), (0., 3.), (1., 2.9), (2., 2.6), (2.6, 2.), (2.9, 1), (3., 0.), (0., 0.)]]], [[[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]]]))
        )",
        R"(
MULTIPOLYGON(((1 2.9,1 4,4 4,4 1,2.9 1,3 0,0 0,0 3,1 2.9)))
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {21, 4};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian = {description_cartesian, syntax_cartesian, arguments_cartesian, {}, returned_value_cartesian, examples_cartesian, introduced_in_cartesian, category_cartesian};

    factory.registerFunction<FunctionPolygonsUnion<CartesianPoint>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(
Calculates a union (OR) of polygons.
    )";
    FunctionDocumentation::Syntax syntax_spherical = "polygonsUnionSpherical(polygon1, polygon2)";
    FunctionDocumentation::Arguments arguments_spherical = {
        {"polygon1", "The first Polygon.", {"Polygon"}},
        {"polygon2", "The second Polygon.", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_spherical = {"Returns the union of the polygons as a MultiPolygon.", {"MultiPolygon"}};
    FunctionDocumentation::Examples examples_spherical =
    {
    {
        "Usage example",
        R"(
SELECT wkt(polygonsUnionSpherical([[[(4.3613577, 50.8651821), (4.349556, 50.8535879), (4.3602419, 50.8435626), (4.3830299, 50.8428851), (4.3904543, 50.8564867), (4.3613148, 50.8651279)]]], [[[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]]))
        )",
        R"(
MULTIPOLYGON(((4.36661 50.8434,4.36623 50.8408,4.34496 50.8333,4.33807 50.8487,4.34669 50.8583,4.35268 50.8567,4.36136 50.8652,4.36131 50.8651,4.39045 50.8565,4.38303 50.8429,4.36661 50.8434)))
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {21, 4};
    FunctionDocumentation::Category category_spherical = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_spherical = {description_spherical, syntax_spherical, arguments_spherical, {}, returned_value_spherical, examples_spherical, introduced_in_spherical, category_spherical};

    factory.registerFunction<FunctionPolygonsUnion<SphericalPoint>>(function_documentation_spherical);
}

}
