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
            ReadBufferFromString in_buffer(std::string_view(str.data, str.size));

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

    static constexpr const char * name = "readWkb";

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
            ReadBufferFromString in_buffer(std::string_view(str.data, str.size));

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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect WKB format value: {}", str.data);

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
    factory.registerFunction<FunctionReadWKB<DataTypePointName, CartesianPoint, PointSerializer<CartesianPoint>, ReadWKBPointNameHolder>>(
        FunctionDocumentation{
            .description = R"(
    Parses a Well-Known Binary (WKB) representation of a Point geometry and returns it in the internal ClickHouse format.
    )",
            .syntax = "readWKBPoint(wkt_string)",
            .arguments{{"wkb_string", "The input WKB string representing a Point geometry."}},
            .returned_value = {"The function returns a ClickHouse internal representation of the point geometry."},
            .examples{
                {"first call",
                 "SELECT "
                 "readWKBPoint(unhex('"
                 "010100000000000000000000000000000000000000"
                 "'));",
                 R"(
    ┌─readWKBPoint(unhex'010100000000000000000000000...'))─┐
    │ (0,0)                                                │
    └──────────────────────────────────────────────────────┘
                )"},
            },
            .introduced_in = {25, 5},
            .category = FunctionDocumentation::Category::Geo,
        });
    factory.registerFunction<
        FunctionReadWKB<DataTypeLineStringName, CartesianLineString, LineStringSerializer<CartesianPoint>, ReadWKBLineStringNameHolder>>(
        FunctionDocumentation{
            .description = R"(
    Parses a Well-Known Binary (WKB) representation of a LineString geometry and returns it in the internal ClickHouse format.
    )",
            .syntax = "readWKBLineString(wkt_string)",
            .arguments{{"wkb_string", "The input WKB string representing a LineString geometry."}},
            .returned_value = {"The function returns a ClickHouse internal representation of the linestring geometry."},
            .examples{
                {"first call",
                 "SELECT "
                 "readWKBLineString(unhex('"
                 "010200000004000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400000000"
                 "00000f03f000000000000f03f'));",
                 R"(
    ┌─readWKBLineString(unhex'0102000000040000000000...'))─┐
    │ [(1,1),(2,2),(3,3),(1,1)]                            │
    └──────────────────────────────────────────────────────┘
                )"},
            },
            .introduced_in = {25, 5},
            .category = FunctionDocumentation::Category::Geo});
    factory.registerFunction<FunctionReadWKB<
        DataTypeMultiLineStringName,
        CartesianMultiLineString,
        MultiLineStringSerializer<CartesianPoint>,
        ReadWKBMultiLineStringNameHolder>>(FunctionDocumentation{
        .description = R"(
            Parses a Well-Known Binary (WKB) representation of a MultiLineString geometry and returns it in the internal ClickHouse format.
            )",
        .syntax = "readWKBMultiLineString(wkt_string)",
        .arguments{{"wkb_string", "The input WKB string representing a MultiLineString geometry."}},
        .returned_value = {"The function returns a ClickHouse internal representation of the multilinestring geometry."},
        .examples{
            {"first call",
             "SELECT "
             "readWKTMultiLineString(unhex('"
             "010500000002000000010200000003000000000000000000f03f000000000000f03f000000000000004000000000000000400000000000000840000000000"
             "0000840010200000003000000000000000000104000000000000010400000000000001440000000000000144000000000000018400000000000001840'))"
             ";",
             R"(
            ┌─readWKBMultiLineString('unhex('010500000002000000010200000003000000000...'))─┐
            │ [[(1,1),(2,2),(3,3)],[(4,4),(5,5),(6,6)]]                                    │
            └──────────────────────────────────────────────────────────────────────────────┘
                        )"},
        },
        .introduced_in = {25, 5},
        .category = FunctionDocumentation::Category::Geo});
    factory.registerFunction<
        FunctionReadWKB<DataTypePolygonName, CartesianPolygon, PolygonSerializer<CartesianPoint>, ReadWKBPolygonNameHolder>>(
        FunctionDocumentation{
            .description = R"(
                Parses a Well-Known Binary (WKB) representation of a Polygon geometry and returns it in the internal ClickHouse format.
                )",
            .syntax = "readWKBPolygon(wkt_string)",
            .arguments{{"wkb_string", "The input WKB string representing a Polygon geometry."}},
            .returned_value = {"The function returns a ClickHouse internal representation of the Polygon geometry."},
            .examples{
                {"first call",
                 "SELECT "
                 "readWKBPolygon(unhex('"
                 "01030000000100000005000000000000000000f03f0000000000000000000000000000244000000000000000000000000000002440000000000000244"
                 "000000000000000000000000000002440000000000000f03f0000000000000000"
                 "'));",
                 R"(
                ┌─readWKBPolygon(unhex'01030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024...'))─┐
                │ (1,0),(10,0),(10,10),(0,10),(1,0)]]                                                                                                 │
                └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                            )"},
            },
            .introduced_in = {25, 5},
            .category = FunctionDocumentation::Category::Geo,
        });
    factory.registerFunction<FunctionReadWKB<
        DataTypeMultiPolygonName,
        CartesianMultiPolygon,
        MultiPolygonSerializer<CartesianPoint>,
        ReadWKBMultiPolygonNameHolder>>(FunctionDocumentation{
        .description = R"(
        Parses a Well-Known Binary (WKB) representation of a MultiPolygon geometry and returns it in the internal ClickHouse format.
        )",
        .syntax = "readWKBMultiPolygon(wkt_string)",
        .arguments{{"wkb_string", "The input WKB string representing a MultiPolygon geometry.", {"String"}}},
        .returned_value = {"The function returns a ClickHouse internal representation of the MultiPolygon geometry."},
        .examples{
            {"first call",
             "SELECT "
             "readWKBMultiPolygon(unhex('"
             "01060000000200000001030000000200000005000000000000000000004000000000000000000000000000002440000000000000000000000000000024400"
             "00000000000244000000000000000000000000000002440000000000000004000000000000000000500000000000000000010400000000000001040000000"
             "00000014400000000000001040000000000000144000000000000014400000000000001040000000000000144000000000000010400000000000001040010"
             "3000000010000000400000000000000000024c000000000000024c000000000000024c000000000000022c000000000000022c00000000000002440000000"
             "00000024c000000000000024c0"
             "'));",
             R"(
        ┌─readWKBMultiPolygon(unhex'01060000000200000001030000000200000005000000000000000000004000000000000000000000000000002440...'))─┐
        │ [[[(2,0),(10,0),(10,10),(0,10),(2,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]],[[(-10,-10),(-10,-9),(-9,10),(-10,-10)]]]             │
        └──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                    )"},
        },
        .introduced_in = {25, 5},
        .category = FunctionDocumentation::Category::Geo,
    }

    );

    factory.registerAlias("ST_PointFromWKB", ReadWKBPointNameHolder::name);
    factory.registerAlias("ST_LineFromWKB", ReadWKBLineStringNameHolder::name);
    factory.registerAlias("ST_MLineFromWKB", ReadWKBMultiLineStringNameHolder::name);
    factory.registerAlias("ST_PolyFromWKB", ReadWKBPolygonNameHolder::name);
    factory.registerAlias("ST_MPolyFromWKB", ReadWKBMultiPolygonNameHolder::name);

    factory.registerFunction<FunctionReadWKBCommon>(
        FunctionDocumentation{
            .description = R"(
    Parses a Well-Known Binary (WKB) representation of a Geometry and returns it in the internal ClickHouse format.
    )",
            .syntax = "readWkb(wkt_string)",
            .arguments{{"wkb_string", "The input WKB string representing a Point geometry."}},
            .returned_value = {"The function returns a ClickHouse internal representation of the Geometry."},
            .examples{
                {"first call",
                 "SELECT "
                 "readWkb(unhex('"
                 "010100000000000000000000000000000000000000"
                 "'));",
                 R"(
    ┌─readWkb(unhex'010100000000000000000000000...'))─┐
    │ (0,0)                                           │
    └─────────────────────────────────────────────────┘
                )"},
            },
            .introduced_in = {25, 7},
            .category = FunctionDocumentation::Category::Geo,
        }
    );
}

}
