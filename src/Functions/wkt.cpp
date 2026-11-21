#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Columns/ColumnString.h>

#include <string>
#include <memory>

namespace DB
{

namespace
{

class FunctionWKT : public IFunction
{
public:
    static inline const char * name = "wkt";

    explicit FunctionWKT() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionWKT>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /*
    * Functions like recursiveRemoveLowCardinality don't pay enough attention to custom types and just erase
    * the information about it during type conversions.
    * While it is a big problem the quick solution would be just to disable default low cardinality implementation
    * because it doesn't make a lot of sense for geo types.
    */
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnString::create();

        callOnGeometryDataType<CartesianPoint>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            auto figures = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::stringstream str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                str.exceptions(std::ios::failbit);
                str << boost::geometry::wkt(figures[i]);
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

REGISTER_FUNCTION(WKT)
{
    FunctionDocumentation::Description description = R"(
Converts a ClickHouse geometry object to its [Well-Known Text (WKT)](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) representation.
    )";
    FunctionDocumentation::Syntax syntax = "wkt(geometry)";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "Geometry object (Point, Ring, Polygon, MultiPolygon).", {"Point", "Ring", "Polygon", "MultiPolygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the WKT string representation of the geometry.", {"String"}};
    FunctionDocumentation::Examples examples = {{"Basic point", "SELECT wkt((0.0, 1.0))", "POINT (0 1)"}};
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionWKT>(documentation);
    factory.registerAlias("WKT", "wkt");
}

}
