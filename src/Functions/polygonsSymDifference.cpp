#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Common/logger_useful.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>

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
class FunctionPolygonsSymDifference : public IFunction
{
public:
    static const char * name;

    explicit FunctionPolygonsSymDifference() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonsSymDifference>();
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

                /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    MultiPolygon<Point> sym_difference{};
                    boost::geometry::sym_difference(first[i], second[i], sym_difference);

                    serializer.add(sym_difference);
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
const char * FunctionPolygonsSymDifference<CartesianPoint>::name = "polygonsSymDifferenceCartesian";

template <>
const char * FunctionPolygonsSymDifference<SphericalPoint>::name = "polygonsSymDifferenceSpherical";

}

REGISTER_FUNCTION(PolygonsSymDifference)
{
    FunctionDocumentation::Description description_cartesian = R"(
The same as [`polygonsSymDifferenceSpherical`](#polygonsSymDifferenceSpherical), but the coordinates are in the Cartesian coordinate system; which is more close to the model of the real Earth.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "polygonsSymDifferenceCartesian(polygon1, polygon2)";
    FunctionDocumentation::Arguments arguments_cartesian = {
        {"polygon1", "The first Polygon.", {"Polygon"}},
        {"polygon2", "The second Polygon", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the symmetric difference of the polygons as a MultiPolygon.", {"MultiPolygon"}};
    FunctionDocumentation::Examples examples_cartesian =
    {
    {
        "Usage example",
        R"(
SELECT wkt(polygonsSymDifferenceCartesian([[[(0, 0), (0, 3), (1, 2.9), (2, 2.6), (2.6, 2), (2.9, 1), (3, 0), (0, 0)]]], [[[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]]]))
        )",
        R"(
MULTIPOLYGON(((1 2.9,1 1,2.9 1,3 0,0 0,0 3,1 2.9)),((1 2.9,1 4,4 4,4 1,2.9 1,2.6 2,2 2.6,1 2.9)))
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {21, 4};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian = {description_cartesian, syntax_cartesian, arguments_cartesian, {}, returned_value_cartesian, examples_cartesian, introduced_in_cartesian, category_cartesian};

    factory.registerFunction<FunctionPolygonsSymDifference<CartesianPoint>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(
Calculates the spatial set theoretic symmetric difference (XOR) between two polygons
    )";
    FunctionDocumentation::Syntax syntax_spherical = "polygonsSymDifferenceSpherical(polygon1, polygon2)";
    FunctionDocumentation::Arguments arguments_spherical = {
        {"polygon1", "The first Polygon.", {"Polygon"}},
        {"polygon2", "The second Polygon", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_spherical = {"Returns the symmetric difference of the polygons as a MultiPolygon.", {"MultiPolygon"}};
    FunctionDocumentation::Examples examples_spherical =
    {
    {
        "Usage example",
        R"(
SELECT wkt(arraySort(polygonsSymDifferenceSpherical([[(50., 50.), (50., -50.), (-50., -50.), (-50., 50.), (50., 50.)], [(10., 10.), (10., 40.), (40., 40.), (40., 10.), (10., 10.)], [(-10., -10.), (-10., -40.), (-40., -40.), (-40., -10.), (-10., -10.)]], [[(-20., -20.), (-20., 20.), (20., 20.), (20., -20.), (-20., -20.)]])));
        )",
        R"(
MULTIPOLYGON(((-20 -10.3067,-10 -10,-10 -20.8791,-20 -20,-20 -10.3067)),((10 20.8791,20 20,20 10.3067,10 10,10 20.8791)),((50 50,50 -50,-50 -50,-50 50,50 50),(20 10.3067,40 10,40 40,10 40,10 20.8791,-20 20,-20 -10.3067,-40 -10,-40 -40,-10 -40,-10 -20.8791,20 -20,20 10.3067)))
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {21, 4};
    FunctionDocumentation::Category category_spherical = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_spherical = {description_spherical, syntax_spherical, arguments_spherical, {}, returned_value_spherical, examples_spherical, introduced_in_spherical, category_spherical};

    factory.registerFunction<FunctionPolygonsSymDifference<SphericalPoint>>(function_documentation_spherical);
}

}
