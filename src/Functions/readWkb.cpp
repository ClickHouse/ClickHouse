#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeCustomGeo.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromString.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Common/Exception.h>
#include <Common/WKB.h>
#include <Functions/geometryConverters.h>
#include <Columns/ColumnVariant.h>

#include <memory>
#include <variant>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <class ReturnDataTypeName, class Geometry, class Serializer, class NameHolder>
class FunctionReadWKB : public IFunction
{
public:
    explicit FunctionReadWKB() = default;

    static constexpr const char * name = NameHolder::name;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkAndGetDataType<DataTypeString>(arguments[0].get()) == nullptr
            && checkAndGetDataType<DataTypeFixedString>(arguments[0].get()) == nullptr)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument should be String");
        }

        return DataTypeFactory::instance().get(ReturnDataTypeName().getName());
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto column = arguments[0].column;
        Serializer serializer;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto str = column->getDataAt(i);
            ReadBufferFromString in_buffer(str);

            auto object = parseWKBFormat(in_buffer);
            auto boost_object = std::get<Geometry>(object);
            serializer.add(boost_object);
        }
        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionReadWKB<ReturnDataTypeName, Geometry, Serializer, NameHolder>>();
    }
};

struct ReadWKBPointNameHolder
{
    static constexpr const char * name = "readWKBPoint";
};

struct ReadWKBLineStringNameHolder
{
    static constexpr const char * name = "readWKBLineString";
};

struct ReadWKBMultiLineStringNameHolder
{
    static constexpr const char * name = "readWKBMultiLineString";
};

struct ReadWKBPolygonNameHolder
{
    static constexpr const char * name = "readWKBPolygon";
};

struct ReadWKBMultiPolygonNameHolder
{
    static constexpr const char * name = "readWKBMultiPolygon";
};

class FunctionReadWKBCommon : public IFunction
{
public:
    enum class WKBTypes
    {
        LineString,
        MultiLineString,
        MultiPolygon,
        Point,
        Polygon,
    };

    explicit FunctionReadWKBCommon() = default;

    static constexpr const char * name = "readWKB";

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkAndGetDataType<DataTypeString>(arguments[0].get()) == nullptr)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument should be String");
        }

        return DataTypeFactory::instance().get("Geometry");
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto column = arguments[0].column;

        PointSerializer<CartesianPoint> point_serializer;
        LineStringSerializer<CartesianPoint> linestring_serializer;
        PolygonSerializer<CartesianPoint> polygon_serializer;
        MultiLineStringSerializer<CartesianPoint> multilinestring_serializer;
        MultiPolygonSerializer<CartesianPoint> multipolygon_serializer;
        RingSerializer<CartesianPoint> ring_serializer;

        auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto str = column->getDataAt(i);
            ReadBufferFromString in_buffer(str);

            auto object = parseWKBFormat(in_buffer);
            UInt8 converted_type = -1;
            if (std::holds_alternative<CartesianPoint>(object))
            {
                point_serializer.add(std::get<CartesianPoint>(object));
                converted_type = static_cast<UInt8>(WKBTypes::Point);
            }
            else if (std::holds_alternative<LineString<CartesianPoint>>(object))
            {
                linestring_serializer.add(std::get<LineString<CartesianPoint>>(object));
                converted_type = static_cast<UInt8>(WKBTypes::LineString);
            }
            else if (std::holds_alternative<Polygon<CartesianPoint>>(object))
            {
                polygon_serializer.add(std::get<Polygon<CartesianPoint>>(object));
                converted_type = static_cast<UInt8>(WKBTypes::Polygon);
            }
            else if (std::holds_alternative<MultiLineString<CartesianPoint>>(object))
            {
                multilinestring_serializer.add(std::get<MultiLineString<CartesianPoint>>(object));
                converted_type = static_cast<UInt8>(WKBTypes::MultiLineString);
            }
            else if (std::holds_alternative<MultiPolygon<CartesianPoint>>(object))
            {
                multipolygon_serializer.add(std::get<MultiPolygon<CartesianPoint>>(object));
                converted_type = static_cast<UInt8>(WKBTypes::MultiPolygon);
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect WKB format value: {}", str);

            discriminators_column->insertValue(converted_type);
        }

        Columns result_columns;
        result_columns.push_back(linestring_serializer.finalize());
        result_columns.push_back(multilinestring_serializer.finalize());
        result_columns.push_back(multipolygon_serializer.finalize());
        result_columns.push_back(point_serializer.finalize());
        result_columns.push_back(polygon_serializer.finalize());
        result_columns.push_back(ring_serializer.finalize());

        return ColumnVariant::create(std::move(discriminators_column), result_columns);
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionReadWKBCommon>();
    }
};

}

REGISTER_FUNCTION(ReadWKB)
{
    FunctionDocumentation::Description description_point = R"(
Parses a Well-Known Binary (WKB) representation of a Point geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_point = "readWKBPoint(wkb_string)";
    FunctionDocumentation::Arguments arguments_point = {
        {"wkb_string", "The input WKB string representing a Point geometry.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_point = {"The function returns a ClickHouse internal representation of the point geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_point =
    {
    {
        "Usage example",
        R"(
SELECT toTypeName(readWKBPoint(unhex('0101000000333333333333f33f3333333333330b40')));
        )",
        R"(
(1.2,3.4)
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_point = {25, 11};
    FunctionDocumentation::Category category_point = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_point = {description_point, syntax_point, arguments_point, {}, returned_value_point, examples_point, introduced_in_point, category_point};

    factory.registerFunction<FunctionReadWKB<DataTypePointName, CartesianPoint, PointSerializer<CartesianPoint>, ReadWKBPointNameHolder>>(function_documentation_point);

    FunctionDocumentation::Description description_linestring = R"(
Parses a Well-Known Binary (WKB) representation of a LineString geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_linestring = "readWKBLineString(wkb_string)";
    FunctionDocumentation::Arguments arguments_linestring = {{"wkb_string", "The input WKB string representing a LineString geometry.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_linestring = {"Returns returns a ClickHouse internal representation of the linestring geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_linestring =
    {
    {
        "Usage example",
        R"(
SELECT readWKBLineString(unhex('010200000004000000000000000000f03f000000000000f03f0000000000000040000000000000004000000000000008400000000000000840000000000000f03f000000000000f03f'));
        )",
        R"(
[(1,1),(2,2),(3,3),(1,1)]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_linestring = {25, 11};
    FunctionDocumentation::Category category_linestring = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_linestring = {description_linestring, syntax_linestring, arguments_linestring, {}, returned_value_linestring, examples_linestring, introduced_in_linestring, category_linestring};

    factory.registerFunction<FunctionReadWKB<DataTypeLineStringName, CartesianLineString, LineStringSerializer<CartesianPoint>, ReadWKBLineStringNameHolder>>(function_documentation_linestring);

    FunctionDocumentation::Description description_multilinestring = R"(
Parses a Well-Known Binary (WKB) representation of a MultiLineString geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_multilinestring = "readWKBMultiLineString(wkb_string)";
    FunctionDocumentation::Arguments arguments_multilinestring = {{"wkb_string", "The input WKB string representing a MultiLineString geometry.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_multilinestring = {"Returns a ClickHouse internal representation of the multilinestring geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_multilinestring =
    {
    {
        "Usage example",
        R"(
SELECT readWKBMultiLineString(unhex('010500000002000000010200000003000000000000000000f03f000000000000f03f0000000000000040000000000000004000000000000008400000000000000840010200000003000000000000000000104000000000000010400000000000001440000000000000144000000000000018400000000000001840'));
        )",
        R"(
[[(1,1),(2,2),(3,3)],[(4,4),(5,5),(6,6)]]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_multilinestring = {25, 11};
    FunctionDocumentation::Category category_multilinestring = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_multilinestring = {description_multilinestring, syntax_multilinestring, arguments_multilinestring, {}, returned_value_multilinestring, examples_multilinestring, introduced_in_multilinestring, category_multilinestring};

    factory.registerFunction<FunctionReadWKB<DataTypeMultiLineStringName, CartesianMultiLineString, MultiLineStringSerializer<CartesianPoint>, ReadWKBMultiLineStringNameHolder>>(function_documentation_multilinestring);

    FunctionDocumentation::Description description_polygon = R"(
Parses a Well-Known Binary (WKB) representation of a Polygon geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_polygon = "readWKBPolygon(wkb_string)";
    FunctionDocumentation::Arguments arguments_polygon = {{"wkb_string", "The input WKB string representing a Polygon geometry.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_polygon = {"Returns a ClickHouse internal representation of the Polygon geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_polygon =
    {
    {
        "Usage example",
        R"(
SELECT
    toTypeName(readWKBPolygon(unhex('010300000001000000050000000000000000000040000000000000000000000000000024400000000000000000000000000000244000000000000024400000000000000000000000000000244000000000000000400000000000000000'))) AS type,
    readWKBPolygon(unhex('010300000001000000050000000000000000000040000000000000000000000000000024400000000000000000000000000000244000000000000024400000000000000000000000000000244000000000000000400000000000000000'));
        )",
        R"(
Polygon [[(2,0),(10,0),(10,10),(0,10),(2,0)]]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_polygon = {25, 11};
    FunctionDocumentation::Category category_polygon = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_polygon = {description_polygon, syntax_polygon, arguments_polygon, {}, returned_value_polygon, examples_polygon, introduced_in_polygon, category_polygon};

    factory.registerFunction<FunctionReadWKB<DataTypePolygonName, CartesianPolygon, PolygonSerializer<CartesianPoint>, ReadWKBPolygonNameHolder>>(function_documentation_polygon);

    FunctionDocumentation::Description description_multipolygon = R"(
Parses a Well-Known Binary (WKB) representation of a MultiPolygon geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_multipolygon = "readWKBMultiPolygon(wkb_string)";
    FunctionDocumentation::Arguments arguments_multipolygon = {{"wkb_string", "The input WKB string representing a MultiPolygon geometry.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_multipolygon = {"Returns a ClickHouse internal representation of the MultiPolygon geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_multipolygon =
    {
    {
        "Usage example",
        R"(
SELECT
    toTypeName(readWKBMultiPolygon(unhex('0106000000020000000103000000020000000500000000000000000000400000000000000000000000000000244000000000000000000000000000002440000000000000244000000000000000000000000000002440000000000000004000000000000000000500000000000000000010400000000000001040000000000000144000000000000010400000000000001440000000000000144000000000000010400000000000001440000000000000104000000000000010400103000000010000000400000000000000000024c000000000000024c000000000000024c000000000000022c000000000000022c0000000000000244000000000000024c000000000000024c0'))) AS type,
    readWKBMultiPolygon(unhex('0106000000020000000103000000020000000500000000000000000000400000000000000000000000000000244000000000000000000000000000002440000000000000244000000000000000000000000000002440000000000000004000000000000000000500000000000000000010400000000000001040000000000000144000000000000010400000000000001440000000000000144000000000000010400000000000001440000000000000104000000000000010400103000000010000000400000000000000000024c000000000000024c000000000000024c000000000000022c000000000000022c0000000000000244000000000000024c000000000000024c0')) FORMAT Vertical;
        )",
        R"(
type:                     MultiPolygon
readWKBMulti~000024c0')): [[[(2,0),(10,0),(10,10),(0,10),(2,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]],[[(-10,-10),(-10,-9),(-9,10),(-10,-10)]]]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_multipolygon = {25, 11};
    FunctionDocumentation::Category category_multipolygon = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_multipolygon = {description_multipolygon, syntax_multipolygon, arguments_multipolygon, {}, returned_value_multipolygon, examples_multipolygon, introduced_in_multipolygon, category_multipolygon};

    factory.registerFunction<FunctionReadWKB<DataTypeMultiPolygonName, CartesianMultiPolygon, MultiPolygonSerializer<CartesianPoint>, ReadWKBMultiPolygonNameHolder>>(function_documentation_multipolygon);

    factory.registerAlias("ST_PointFromWKB", ReadWKBPointNameHolder::name);
    factory.registerAlias("ST_LineFromWKB", ReadWKBLineStringNameHolder::name);
    factory.registerAlias("ST_MLineFromWKB", ReadWKBMultiLineStringNameHolder::name);
    factory.registerAlias("ST_PolyFromWKB", ReadWKBPolygonNameHolder::name);
    factory.registerAlias("ST_MPolyFromWKB", ReadWKBMultiPolygonNameHolder::name);

    FunctionDocumentation::Description description_common = R"(
Parses a Well-Known Binary (WKB) representation of a Geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_common = "readWKB(wkb_string)";
    FunctionDocumentation::Arguments arguments_common = {{"wkb_string", "The input WKB string representing a Point geometry.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_common = {"Returns a ClickHouse internal representation of the Geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_common =
    {
    {
        "Usage example",
        R"(
SELECT readWKB(unhex('010300000001000000050000000000000000000040000000000000000000000000000024400000000000000000000000000000244000000000000024400000000000000000000000000000244000000000000000400000000000000000'));
        )",
        R"(
[[(2,0),(10,0),(10,10),(0,10),(2,0)]]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_common = {25, 12};
    FunctionDocumentation::Category category_common = FunctionDocumentation::Category::Geo;
    FunctionDocumentation function_documentation_common = {description_common, syntax_common, arguments_common, {}, returned_value_common, examples_common, introduced_in_common, category_common};

    factory.registerFunction<FunctionReadWKBCommon>(function_documentation_common);

    /// This was initially added by mistake, but we have to keep it:
    factory.registerAlias("readWkb", "readWKB");
}

}
