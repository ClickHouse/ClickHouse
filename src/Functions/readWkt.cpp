#include <Columns/ColumnString.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeCustomGeo.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/geometryConverters.h>
#include <boost/algorithm/string/case_conv.hpp>

#include <string>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

class FunctionReadWKTCommon : public IFunction
{
public:
    enum class WKTTypes
    {
        LineString,
        MultiLineString,
        MultiPolygon,
        Point,
        Polygon,
        Ring,
    };

    explicit FunctionReadWKTCommon() = default;

    static constexpr const char * name = "readWkt";

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

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
        const auto & column_string = checkAndGetColumn<ColumnString>(*arguments[0].column);

        PointSerializer<CartesianPoint> point_serializer;
        LineStringSerializer<CartesianPoint> linestring_serializer;
        PolygonSerializer<CartesianPoint> polygon_serializer;
        MultiLineStringSerializer<CartesianPoint> multilinestring_serializer;
        MultiPolygonSerializer<CartesianPoint> multipolygon_serializer;
        RingSerializer<CartesianPoint> ring_serializer;

        auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();

        auto try_deserialize_type = [&] (const std::function<void()> & deserialize_func, const String & data, const String & target_prefix, WKTTypes type) -> bool
        {
            auto lower_data = boost::to_lower_copy(data);
            if (lower_data.starts_with(target_prefix))
            {
                deserialize_func();
                UInt8 converted_type = static_cast<UInt8>(type);
                discriminators_column->insertValue(converted_type);
                return true;
            }
            return false;
        };
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto & str = column_string.getDataAt(i).toString();
            if (try_deserialize_type(
                    [&]
                    {
                        CartesianPoint point;
                        boost::geometry::read_wkt(str, point);
                        point_serializer.add(point);
                    },
                    str, "point", WKTTypes::Point))
                continue;

            if (try_deserialize_type(
                    [&]
                    {
                        LineString<CartesianPoint> linestring;
                        boost::geometry::read_wkt(str, linestring);
                        linestring_serializer.add(linestring);
                    },
                    str, "linestring", WKTTypes::LineString))
                continue;

            if (try_deserialize_type(
                    [&]
                    {
                        Polygon<CartesianPoint> polygon;
                        boost::geometry::read_wkt(str, polygon);
                        polygon_serializer.add(polygon);
                    },
                    str, "polygon", WKTTypes::Polygon))
                continue;

            if (try_deserialize_type(
                    [&]
                    {
                        MultiLineString<CartesianPoint> multilinestring;
                        boost::geometry::read_wkt(str, multilinestring);
                        multilinestring_serializer.add(multilinestring);
                    },
                    str, "multilinestring", WKTTypes::MultiLineString))
                continue;

            if (try_deserialize_type(
                    [&]
                    {
                        MultiPolygon<CartesianPoint> multipolygon;
                        boost::geometry::read_wkt(str, multipolygon);
                        multipolygon_serializer.add(multipolygon);
                    },
                    str, "multipolygon", WKTTypes::MultiPolygon))
                continue;

            if (try_deserialize_type(
                    [&]
                    {
                        Ring<CartesianPoint> ring;
                        boost::geometry::read_wkt(str, ring);
                        ring_serializer.add(ring);
                    },
                    str, "ring", WKTTypes::Ring))
                continue;

            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect WKT format value: {}", str);
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

    bool useDefaultImplementationForConstants() const override { return true; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReadWKTCommon>(); }
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
            {"wkt_string", "The input WKT string representing a LineString geometry.", {"String"}}
        },
        .returned_value = {"The function returns a ClickHouse internal representation of the linestring geometry."},
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
        .category = FunctionDocumentation::Category::UUID
    });
    factory.registerFunction<FunctionReadWKT<DataTypeMultiLineStringName, CartesianMultiLineString, MultiLineStringSerializer<CartesianPoint>, ReadWKTMultiLineStringNameHolder>>(FunctionDocumentation
    {
        .description=R"(
Parses a Well-Known Text (WKT) representation of a MultiLineString geometry and returns it in the internal ClickHouse format.
)",
        .syntax = "readWKTMultiLineString(wkt_string)",
        .arguments{
            {"wkt_string", "The input WKT string representing a MultiLineString geometry.", {"String"}}
        },
        .returned_value = {"The function returns a ClickHouse internal representation of the multilinestring geometry."},
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
        .category = FunctionDocumentation::Category::Geo
    });
    factory.registerFunction<FunctionReadWKT<DataTypeRingName, CartesianRing, RingSerializer<CartesianPoint>, ReadWKTRingNameHolder>>();
    factory.registerFunction<FunctionReadWKT<DataTypePolygonName, CartesianPolygon, PolygonSerializer<CartesianPoint>, ReadWKTPolygonNameHolder>>();
    factory.registerFunction<FunctionReadWKT<DataTypeMultiPolygonName, CartesianMultiPolygon, MultiPolygonSerializer<CartesianPoint>, ReadWKTMultiPolygonNameHolder>>();

    factory.registerFunction<FunctionReadWKTCommon>(FunctionDocumentation{
        .description = R"(
Parses a Well-Known Text (WKT) representation of Geometry and returns it in the internal ClickHouse format.
)",
        .syntax = "readWkt(wkt_string)",
        .arguments{{"wkt_string", "The input WKT string representing a LineString geometry.", {"String"}}},
        .returned_value = {"The function returns a ClickHouse internal representation of the Geometry."},
        .examples{
            {"first call", "SELECT readWkt('LINESTRING (1 1, 2 2, 3 3, 1 1)');", R"(
┌─readWkt('LINESTRING (1 1, 2 2, 3 3, 1 1)')─┐
│ [(1,1),(2,2),(3,3),(1,1)]                  │
└────────────────────────────────────────────┘
            )"},
        },
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Geo});
}

}
