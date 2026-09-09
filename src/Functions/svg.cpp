#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/geometryConverters.h>
#include <Columns/ColumnString.h>

#include <string>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionSvg : public IFunction
{
public:
    static inline const char * name = "svg";

    explicit FunctionSvg() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionSvg>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments: expected 1 or 2 arguments");
        }
        if (arguments.size() == 2 && checkAndGetDataType<DataTypeString>(arguments[1].get()) == nullptr)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument should be String");
        }

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnString::create();
        bool has_style = arguments.size() > 1;
        ColumnPtr style;
        if (has_style)
            style = arguments[1].column;

        callOnGeometryDataType<CartesianPoint>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            auto figures = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::stringstream str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                boost::geometry::correct(figures[i]);
                str << boost::geometry::svg(figures[i], has_style ? std::string{style->getDataAt(i)} : "");
                std::string serialized = str.str();
                res_column->insertData(serialized.data(), serialized.size());
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

}

REGISTER_FUNCTION(Svg)
{
    FunctionDocumentation::Description description = R"(
Returns a string representation of a geometry in [SVG](https://en.wikipedia.org/wiki/SVG) format. The output SVG can be used directly in web pages to visualize geospatial data.
    )";
    FunctionDocumentation::Syntax syntax = "svg(geometry[, style])";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "Geometry object (Point, Ring, Polygon, MultiPolygon).", {"Point", "Ring", "Polygon", "MultiPolygon"}},
        {"style", "Optional CSS style string to apply to the SVG element.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the SVG representation of the geometry.", {"String"}};
    FunctionDocumentation::Examples examples = {{"Basic point", "SELECT svg((0.0, 1.0))", R"(<circle cx="0" cy="-1" r="5"/>)"}};
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSvg>(documentation);
    factory.registerAlias("SVG", "svg");
}

}
