#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <memory>
#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Point>
class FunctionPolygonPerimeter : public IFunction
{
public:
    static const char * name;

    explicit FunctionPolygonPerimeter() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonPerimeter>();
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

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnFloat64::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        callOnGeometryDataType<Point>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            if constexpr (std::is_same_v<ColumnToPointsConverter<Point>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must not be Point", getName());
            else
            {
                auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                for (size_t i = 0; i < input_rows_count; ++i)
                    res_data.emplace_back(static_cast<Float64>(boost::geometry::perimeter(geometries[i])));
            }
        });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonPerimeter<CartesianPoint>::name = "polygonPerimeterCartesian";

template <>
const char * FunctionPolygonPerimeter<SphericalPoint>::name = "polygonPerimeterSpherical";

}

REGISTER_FUNCTION(PolygonPerimeter)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates the perimeter of a polygon.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "polygonPerimeterCartesian(polygon)";
    FunctionDocumentation::Arguments arguments_cartesian = {
        {"polygon", "A Polygon value.", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the perimeter of the polygon.", {"Float64"}};
    FunctionDocumentation::Examples examples_cartesian =
    {
    {
        "Usage example",
        R"(
SELECT polygonPerimeterCartesian([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]])
        )",
        R"(
15
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {21, 4};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian = {description_cartesian, syntax_cartesian, arguments_cartesian, {}, returned_value_cartesian, examples_cartesian, introduced_in_cartesian, category_cartesian};

    factory.registerFunction<FunctionPolygonPerimeter<CartesianPoint>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(Calculates the perimeter of the polygon.)";
    FunctionDocumentation::Syntax syntax_spherical = "polygonPerimeterSpherical(polygon)";
    FunctionDocumentation::Arguments arguments_spherical = {{"polygon", "A value of type [`Polygon`](/sql-reference/data-types/geo#polygon)"}};
    FunctionDocumentation::ReturnedValue returned_value_spherical = {"The perimeter of the polygon on a sphere", {"Float64"}};
    FunctionDocumentation::Examples examples_spherical = {{"spherical_example",
                  R"(
SELECT round(polygonPerimeterSpherical([[[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]]), 6)
                  )",
                  R"(
0.045539
                  )"}};
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {21, 4};
    FunctionDocumentation::Category category_spherical = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_spherical = {description_spherical, syntax_spherical, arguments_spherical, {}, returned_value_spherical, examples_spherical, introduced_in_spherical, category_spherical};

    factory.registerFunction<FunctionPolygonPerimeter<SphericalPoint>>(function_documentation_spherical);
}


}
