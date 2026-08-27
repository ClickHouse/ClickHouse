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
            const std::string str{column_string.getDataAt(i)};
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

    static constexpr const char * name = "readWKT";

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
            const std::string str{column_string.getDataAt(i)};
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
    FunctionDocumentation::Description description_point = R"(
The `readWKTPoint` function in ClickHouse parses a Well-Known Text (WKT) representation of a Point geometry and returns a point in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_point = "readWKTPoint(wkt_string)";
    FunctionDocumentation::Arguments arguments_point = {{"wkt_string", "The input WKT string representing a Point geometry.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_point = {"Returns a ClickHouse internal representation of the Point geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_point = {{"Usage example", "SELECT readWKTPoint('POINT (1.2 3.4)');", "(1.2,3.4)"}};
    FunctionDocumentation::IntroducedIn introduced_in_point = {21, 4};
    FunctionDocumentation::Category category_point = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_point = {description_point, syntax_point, arguments_point, {}, returned_value_point, examples_point, introduced_in_point, category_point};

    factory.registerFunction<FunctionReadWKT<DataTypePointName, CartesianPoint, PointSerializer<CartesianPoint>, ReadWKTPointNameHolder>>(function_documentation_point);

    FunctionDocumentation::Description description_linestring = R"(
Parses a Well-Known Text (WKT) representation of a LineString geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_linestring = "readWKTLineString(wkt_string)";
    FunctionDocumentation::Arguments arguments_linestring = {
        {"wkt_string", "The input WKT string representing a LineString geometry.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_linestring = {"Returns a ClickHouse internal representation of the linestring geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_linestring =
    {
    {
        "Usage example",
        "SELECT readWKTLineString('LINESTRING (1 1, 2 2, 3 3, 1 1)');",
        R"(
┌─readWKTLineString('LINESTRING (1 1, 2 2, 3 3, 1 1)')─┐
│ [(1,1),(2,2),(3,3),(1,1)]                            │
└──────────────────────────────────────────────────────┘
        )"
    },
    {
        "LineString example",
        "SELECT toTypeName(readWKTLineString('LINESTRING (1 1, 2 2, 3 3, 1 1)'));",
        R"(
┌─toTypeName(readWKTLineString('LINESTRING (1 1, 2 2, 3 3, 1 1)'))─┐
│ LineString                                                       │
└──────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_linestring = {21, 4};
    FunctionDocumentation::Category category_linestring = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_linestring = {description_linestring, syntax_linestring, arguments_linestring, {}, returned_value_linestring, examples_linestring, introduced_in_linestring, category_linestring};

    factory.registerFunction<FunctionReadWKT<DataTypeLineStringName, CartesianLineString, LineStringSerializer<CartesianPoint>, ReadWKTLineStringNameHolder>>(function_documentation_linestring);

    FunctionDocumentation::Description description_multilinestring = R"(
Parses a Well-Known Text (WKT) representation of a MultiLineString geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_multilinestring = "readWKTMultiLineString(wkt_string)";
    FunctionDocumentation::Arguments arguments_multilinestring = {
        {"wkt_string", "The input WKT string representing a MultiLineString geometry.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_multilinestring = {"Returns the function returns a ClickHouse internal representation of the multilinestring geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_multilinestring = {
    {
        "Usage example",
        "SELECT readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3), (4 4, 5 5, 6 6))');",
        R"(
┌─readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3), (4 4, 5 5, 6 6))')─┐
│ [[(1,1),(2,2),(3,3)],[(4,4),(5,5),(6,6)]]                                    │
└──────────────────────────────────────────────────────────────────────────────┘

        )"
    },
    {
        "MultiLineString example",
        "SELECT toTypeName(readWKTLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1))'));",
        R"(
┌─toTypeName(readWKTLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1))'))─┐
│ MultiLineString                                                         │
└─────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_multilinestring = {21, 4};
    FunctionDocumentation::Category category_multilinestring = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_multilinestring = {description_multilinestring, syntax_multilinestring, arguments_multilinestring, {}, returned_value_multilinestring, examples_multilinestring, introduced_in_multilinestring, category_multilinestring};

    factory.registerFunction<FunctionReadWKT<DataTypeMultiLineStringName, CartesianMultiLineString, MultiLineStringSerializer<CartesianPoint>, ReadWKTMultiLineStringNameHolder>>(function_documentation_multilinestring);

    FunctionDocumentation::Description description_ring = R"(
Parses a Well-Known Text (WKT) representation of a Polygon geometry and returns a ring (closed linestring) in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_ring = "readWKTRing(wkt_string)";
    FunctionDocumentation::Arguments arguments_ring = {{"wkt_string", "The input WKT string representing a Polygon geometry.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_ring = {"Returns a ClickHouse internal representation of the ring (closed linestring) geometry.", {"Geo"}};
    FunctionDocumentation::Examples examples_ring = {{"Usage example", "SELECT readWKTRing('POLYGON ((1 1, 2 2, 3 3, 1 1))');", "[(1,1),(2,2),(3,3),(1,1)]"}};
    FunctionDocumentation::IntroducedIn introduced_in_ring = {21, 4};
    FunctionDocumentation::Category category_ring = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_ring = {description_ring, syntax_ring, arguments_ring, {}, returned_value_ring, examples_ring, introduced_in_ring, category_ring};

    factory.registerFunction<FunctionReadWKT<DataTypeRingName, CartesianRing, RingSerializer<CartesianPoint>, ReadWKTRingNameHolder>>(function_documentation_ring);

    FunctionDocumentation::Description description_polygon = R"(
Converts a WKT (Well Known Text) MultiPolygon into a Polygon type.
    )";
    FunctionDocumentation::Syntax syntax_polygon = "readWKTPolygon(wkt_string)";
    FunctionDocumentation::Arguments arguments_polygon = {{"wkt_string", "String starting with `POLYGON`", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_polygon = {"Returns a Polygon", {"Polygon"}};
    FunctionDocumentation::Examples examples_polygon =
    {
    {
        "Usage example",
        R"(
SELECT
    toTypeName(readWKTPolygon('POLYGON((2 0,10 0,10 10,0 10,2 0))')) AS type,
    readWKTPolygon('POLYGON((2 0,10 0,10 10,0 10,2 0))') AS output
        )",
        R"(
[[(2,0),(10,0),(10,10),(0,10),(2,0)]]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_polygon = {21, 4};
    FunctionDocumentation::Category category_polygon = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_polygon = {description_polygon, syntax_polygon, arguments_polygon, {}, returned_value_polygon, examples_polygon, introduced_in_polygon, category_polygon};

    factory.registerFunction<FunctionReadWKT<DataTypePolygonName, CartesianPolygon, PolygonSerializer<CartesianPoint>, ReadWKTPolygonNameHolder>>(function_documentation_polygon);

    FunctionDocumentation::Description description_multipolygon = R"(
Converts a WKT (Well Known Text) MultiPolygon into a MultiPolygon type.
    )";
    FunctionDocumentation::Syntax syntax_multipolygon = "readWKTMultiPolygon(wkt_string)";
    FunctionDocumentation::Arguments arguments_multipolygon = {{"wkt_string", "String starting with `MULTIPOLYGON`", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_multipolygon = {"Returns a MultiPolygon", {"MultiPolygon"}};
    FunctionDocumentation::Examples examples_multipolygon =
    {
    {
        "Usage example",
        R"(
SELECT
    toTypeName(readWKTMultiPolygon('MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))')) AS type,
    readWKTMultiPolygon('MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))') AS output
        )",
        R"(
[[[(2,0),(10,0),(10,10),(0,10),(2,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]],[[(-10,-10),(-10,-9),(-9,10),(-10,-10)]]]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_multipolygon = {21, 4};
    FunctionDocumentation::Category category_multipolygon = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_multipolygon = {description_multipolygon, syntax_multipolygon, arguments_multipolygon, {}, returned_value_multipolygon, examples_multipolygon, introduced_in_multipolygon, category_multipolygon};

    factory.registerFunction<FunctionReadWKT<DataTypeMultiPolygonName, CartesianMultiPolygon, MultiPolygonSerializer<CartesianPoint>, ReadWKTMultiPolygonNameHolder>>(function_documentation_multipolygon);

    FunctionDocumentation::Description description_common = R"(
Parses a Well-Known Text (WKT) representation of Geometry and returns it in the internal ClickHouse format.
    )";
    FunctionDocumentation::Syntax syntax_common = "readWKT(wkt_string)";
    FunctionDocumentation::Arguments arguments_common = {{"wkt_string", "The input WKT string representing a LineString geometry.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_common = {"Returns a ClickHouse internal representation of the Geometry."};
    FunctionDocumentation::Examples examples_common =
    {
    {
        "Usage example",
        "SELECT readWKT('LINESTRING (1 1, 2 2, 3 3, 1 1)');",
        R"(
┌─readWKT('LINESTRING (1 1, 2 2, 3 3, 1 1)')─┐
│ [(1,1),(2,2),(3,3),(1,1)]                  │
└────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_common = {25, 7};
    FunctionDocumentation::Category category_common = FunctionDocumentation::Category::Geo;
    FunctionDocumentation function_documentation_common = {description_common, syntax_common, arguments_common, {}, returned_value_common, examples_common, introduced_in_common, category_common};

    factory.registerFunction<FunctionReadWKTCommon>(function_documentation_common);

    /// This was initially added by mistake, but we have to keep it:
    factory.registerAlias("readWkt", "readWKT");
}

}
