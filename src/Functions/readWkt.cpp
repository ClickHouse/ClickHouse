#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeCustomGeo.h>
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
}

namespace
{

template <class DataTypeName, class Geometry, class Serializer, class NameHolder>
class FunctionReadWKT : public IFunction
{
public:
    explicit FunctionReadWKT() = default;

    static constexpr const char * name = NameHolder::name;

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

        return DataTypeFactory::instance().get(DataTypeName().getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto & column_string = checkAndGetColumn<ColumnString>(*arguments[0].column);

        Serializer serializer;
        Geometry geometry;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto & str = column_string.getDataAt(i).toString();
            boost::geometry::read_wkt(str, geometry);
            serializer.add(geometry);
        }

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionReadWKT<DataTypeName, Geometry, Serializer, NameHolder>>();
    }
};

struct ReadWKTPointNameHolder
{
    static constexpr const char * name = "readWKTPoint";
};

struct ReadWKTLineStringNameHolder
{
    static constexpr const char * name = "readWKTLineString";
};

struct ReadWKTMultiLineStringNameHolder
{
    static constexpr const char * name = "readWKTMultiLineString";
};

struct ReadWKTRingNameHolder
{
    static constexpr const char * name = "readWKTRing";
};

struct ReadWKTPolygonNameHolder
{
    static constexpr const char * name = "readWKTPolygon";
};

struct ReadWKTMultiPolygonNameHolder
{
    static constexpr const char * name = "readWKTMultiPolygon";
};

}

REGISTER_FUNCTION(ReadWKT)
{
    factory.registerFunction<FunctionReadWKT<DataTypePointName, CartesianPoint, PointSerializer<CartesianPoint>, ReadWKTPointNameHolder>>();
    factory.registerFunction<FunctionReadWKT<DataTypeLineStringName, CartesianLineString, LineStringSerializer<CartesianPoint>, ReadWKTLineStringNameHolder>>(FunctionDocumentation
    {
        .description=R"(
Parses a Well-Known Text (WKT) representation of a LineString geometry and returns it in the internal ClickHouse format.
)",
        .syntax = "readWKTLineString(wkt_string)",
        .arguments{
            {"wkt_string", "The input WKT string representing a LineString geometry."}
        },
        .returned_value = "The function returns a ClickHouse internal representation of the linestring geometry.",
        .examples{
            {"first call", "SELECT readWKTLineString('LINESTRING (1 1, 2 2, 3 3, 1 1)');", R"(
┌─readWKTLineString('LINESTRING (1 1, 2 2, 3 3, 1 1)')─┐
│ [(1,1),(2,2),(3,3),(1,1)]                            │
└──────────────────────────────────────────────────────┘
            )"},
            {"second call", "SELECT toTypeName(readWKTLineString('LINESTRING (1 1, 2 2, 3 3, 1 1)'));", R"(
┌─toTypeName(readWKTLineString('LINESTRING (1 1, 2 2, 3 3, 1 1)'))─┐
│ LineString                                                       │
└──────────────────────────────────────────────────────────────────┘
            )"},
        },
        .categories{"Unique identifiers"}
    });
    factory.registerFunction<FunctionReadWKT<DataTypeMultiLineStringName, CartesianMultiLineString, MultiLineStringSerializer<CartesianPoint>, ReadWKTMultiLineStringNameHolder>>(FunctionDocumentation
    {
        .description=R"(
Parses a Well-Known Text (WKT) representation of a MultiLineString geometry and returns it in the internal ClickHouse format.
)",
        .syntax = "readWKTMultiLineString(wkt_string)",
        .arguments{
            {"wkt_string", "The input WKT string representing a MultiLineString geometry."}
        },
        .returned_value = "The function returns a ClickHouse internal representation of the multilinestring geometry.",
        .examples{
            {"first call", "SELECT readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3), (4 4, 5 5, 6 6))');", R"(
┌─readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3), (4 4, 5 5, 6 6))')─┐
│ [[(1,1),(2,2),(3,3)],[(4,4),(5,5),(6,6)]]                                    │
└──────────────────────────────────────────────────────────────────────────────┘

            )"},
            {"second call", "SELECT toTypeName(readWKTLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1))'));", R"(
┌─toTypeName(readWKTLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1))'))─┐
│ MultiLineString                                                         │
└─────────────────────────────────────────────────────────────────────────┘
            )"},
        },
        .categories{"Unique identifiers"}
    });
    factory.registerFunction<FunctionReadWKT<DataTypeRingName, CartesianRing, RingSerializer<CartesianPoint>, ReadWKTRingNameHolder>>();
    factory.registerFunction<FunctionReadWKT<DataTypePolygonName, CartesianPolygon, PolygonSerializer<CartesianPoint>, ReadWKTPolygonNameHolder>>();
    factory.registerFunction<FunctionReadWKT<DataTypeMultiPolygonName, CartesianMultiPolygon, MultiPolygonSerializer<CartesianPoint>, ReadWKTMultiPolygonNameHolder>>();
}

}
