#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>


#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

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
class FunctionPolygonArea : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonArea() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonArea>();
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
                    res_data.emplace_back(boost::geometry::area(geometries[i]));
            }
        }
        );

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonArea<CartesianPoint>::name = "polygonAreaCartesian";

template <>
const char * FunctionPolygonArea<SphericalPoint>::name = "polygonAreaSpherical";

}

REGISTER_FUNCTION(PolygonArea)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates the area of a polygon.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "polygonAreaCartesian(polygon)";
    FunctionDocumentation::Arguments arguments_cartesian = {{"polygon", "A polygon value", {"Polygon"}}};
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the area of the polygon", {"Float64"}};
    FunctionDocumentation::Examples examples_cartesian =
    {
    {
        "Usage example",
        R"(
SELECT polygonAreaCartesian([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]])
        )",
        R"(
25
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {21, 4};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian = {description_cartesian, syntax_cartesian, arguments_cartesian, {}, returned_value_cartesian, examples_cartesian, introduced_in_cartesian, category_cartesian};

    factory.registerFunction<FunctionPolygonArea<CartesianPoint>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(
Calculates the surface area of a polygon.
    )";
    FunctionDocumentation::Syntax syntax_spherical = "polygonAreaSpherical(polygon)";
    FunctionDocumentation::Arguments arguments_spherical = {{"polygon", "A polygon value.", {"Polygon"}}};
    FunctionDocumentation::ReturnedValue returned_value_spherical = {"Returns the surface area of the polygon", {"Float64"}};
    FunctionDocumentation::Examples examples_spherical =
    {
    {
        "Spherical example",
        R"(
SELECT round(polygonAreaSpherical([[[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]]), 14)
        )",
        R"(
9.387704e-8
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {21, 4};
    FunctionDocumentation::Category category_spherical = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_spherical = {description_spherical, syntax_spherical, arguments_spherical, {}, returned_value_spherical, examples_spherical, introduced_in_spherical, category_spherical};

    factory.registerFunction<FunctionPolygonArea<SphericalPoint>>(function_documentation_spherical);
}


}
