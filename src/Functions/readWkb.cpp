#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeCustomGeo.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/geometryConverters.h>

#include "Common/Exception.h"
#include <Common/WKB.h>

#include <memory>
#include <string>
#include <type_traits>
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

template <typename T>
T convertGeometricObjectToBoostObject(const ArrowGeometricObject & object)
requires std::is_same_v<T, CartesianPoint>
{
    if (!std::holds_alternative<ArrowPoint>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not convert to boost object - expected point");
    const auto & point = std::get<ArrowPoint>(object);
    return CartesianPoint(point.x, point.y);
}

template <typename T>
T convertGeometricObjectToBoostObject(const ArrowGeometricObject & object)
requires std::is_same_v<T, CartesianLineString>
{
    if (!std::holds_alternative<ArrowLineString>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not convert to boost object - expected linestring");
    const auto & linestring = std::get<ArrowLineString>(object);

    CartesianLineString result;
    result.reserve(linestring.size());
    for (const auto & point : linestring)
        result.push_back(convertGeometricObjectToBoostObject<CartesianPoint>(point));
    return result;
}

template <typename T>
T convertGeometricObjectToBoostObject(const ArrowGeometricObject & object)
requires std::is_same_v<T, CartesianPolygon>
{
    if (!std::holds_alternative<ArrowPolygon>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not convert to boost object - expected polygon");

    const auto & polygon = std::get<ArrowPolygon>(object);
    CartesianPolygon result;
    result.outer().reserve(polygon[0].size());
    for (const auto & point : polygon[0])
        result.outer().push_back(convertGeometricObjectToBoostObject<CartesianPoint>(point));

    result.inners().reserve(polygon.size() - 1);
    for (size_t i = 1; i < polygon.size(); ++i)
    {
        result.inners().push_back({});
        for (const auto & point : polygon[i])
            result.inners().back().push_back(convertGeometricObjectToBoostObject<CartesianPoint>(point));
    }
    return result;
}

template <typename T>
T convertGeometricObjectToBoostObject(const ArrowGeometricObject & object)
requires std::is_same_v<T, CartesianMultiLineString>
{
    if (!std::holds_alternative<ArrowMultiLineString>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not convert to boost object - expected point");
    const auto & multilinestring = std::get<ArrowMultiLineString>(object);

    CartesianMultiLineString result;
    result.reserve(multilinestring.size());
    for (const auto & linestring : multilinestring)
        result.push_back(convertGeometricObjectToBoostObject<CartesianLineString>(linestring));
    return result;
}

template <typename T>
T convertGeometricObjectToBoostObject(const ArrowGeometricObject & object)
requires std::is_same_v<T, CartesianMultiPolygon>
{
    if (!std::holds_alternative<ArrowMultiPolygon>(object))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not convert to boost object - expected point");
    const auto & multipolygon = std::get<ArrowMultiPolygon>(object);

    CartesianMultiPolygon result;
    result.reserve(multipolygon.size());
    for (const auto & polygon : multipolygon)
        result.push_back(convertGeometricObjectToBoostObject<CartesianPolygon>(polygon));
    return result;
}

template <class DataTypeName, class Geometry, class Serializer, class NameHolder>
class FunctionReadWKB : public IFunction
{
public:
    explicit FunctionReadWKB() = default;

    static constexpr const char * name = NameHolder::name;

    String getName() const override { return "readWKB"; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkAndGetDataType<DataTypeString>(arguments[0].get()) == nullptr)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument should be String");
        }

        return DataTypeFactory::instance().get(DataTypeName().getName());
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto & column_string = checkAndGetColumn<ColumnString>(*arguments[0].column);

        Serializer serializer;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto str = column_string.getDataAt(i).toString();
            ReadBuffer in_buffer(str.data(), str.size(), 0);

            auto object = parseWKBFormat(in_buffer);
            auto boost_object = convertGeometricObjectToBoostObject<Geometry>(object);
            serializer.add(boost_object);
        }

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReadWKB<DataTypeName, Geometry, Serializer, NameHolder>>(); }
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

}

REGISTER_FUNCTION(ReadWKB)
{
    factory.registerFunction<FunctionReadWKB<DataTypePointName, CartesianPoint, PointSerializer<CartesianPoint>, ReadWKBPointNameHolder>>();
    factory.registerFunction<
        FunctionReadWKB<DataTypeLineStringName, CartesianLineString, LineStringSerializer<CartesianPoint>, ReadWKBLineStringNameHolder>>(
        FunctionDocumentation{
            .description = R"(
    Parses a Well-Known Binary (WKB) representation of a LineString geometry and returns it in the internal ClickHouse format.
    )",
            .syntax = "readWKBLineString(wkt_string)",
            .arguments{{"wkb_string", "The input WKB string representing a LineString geometry."}},
            .returned_value = "The function returns a ClickHouse internal representation of the linestring geometry.",
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
            .category = FunctionDocumentation::Category::UUID});
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
        .returned_value = "The function returns a ClickHouse internal representation of the multilinestring geometry.",
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
        .category = FunctionDocumentation::Category::Geo});
    factory.registerFunction<
        FunctionReadWKB<DataTypePolygonName, CartesianPolygon, PolygonSerializer<CartesianPoint>, ReadWKBPolygonNameHolder>>();
    factory.registerFunction<FunctionReadWKB<
        DataTypeMultiPolygonName,
        CartesianMultiPolygon,
        MultiPolygonSerializer<CartesianPoint>,
        ReadWKBMultiPolygonNameHolder>>();
}

}
