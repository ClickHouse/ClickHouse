#include <Functions/FunctionFactory.h>
#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>

#include <Columns/ColumnVariant.h>

#include <base/EnumReflection.h>

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

template <typename Traits>
class FunctionCentroid : public IFunction
{
public:
    using Point = typename Traits::PointType;
    static constexpr auto name = Traits::name;

    explicit FunctionCentroid() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCentroid>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->getName() != "Geometry" && !getGeometryColumnTypeFromDataType(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} should be a geometry type, got {}",
                getName(),
                arguments[0]->getName());

        return DataTypeFactory::instance().get("Point");
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForVariant() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        PointSerializer<Point> serializer;

        const auto * column_variant = typeid_cast<const ColumnVariant *>(arguments.front().column.get());
        if (column_variant)
        {
            Field field;
            const auto & descriptors = column_variant->getLocalDiscriminators();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                column_variant->get(i, field);
                auto type = magic_enum::enum_cast<GeometryColumnType>(descriptors[i]);
                if (!type)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of geometry {}", static_cast<Int32>(descriptors[i]));
                processField(field, *type, serializer);
            }
        }
        else
        {
            /// Individual geometry type path.
            auto geo_type = getGeometryColumnTypeFromDataType(arguments.front().type);
            if (geo_type)
            {
                Field field;
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    arguments.front().column->get(i, field);
                    processField(field, *geo_type, serializer);
                }
            }
            else
            {
                callOnGeometryDataType<Point>(
                    arguments[0].type,
                    [&](const auto & type)
                    {
                        using TypeConverter = std::decay_t<decltype(type)>;
                        using Converter = typename TypeConverter::Type;

                        auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                        for (size_t i = 0; i < input_rows_count; ++i)
                        {
                            Point centroid{};
                            boost::geometry::centroid(geometries[i], centroid);
                            serializer.add(centroid);
                        }
                    });
            }
        }

        return serializer.finalize();
    }

private:
    template <typename Geom>
    static void computeCentroid(const Geom & geom, PointSerializer<Point> & serializer)
    {
        Point centroid{};
        boost::geometry::centroid(geom, centroid);
        serializer.add(centroid);
    }

    static void processField(const Field & field, GeometryColumnType type, PointSerializer<Point> & serializer)
    {
        switch (type)
        {
            case GeometryColumnType::Point: {
                /// Centroid of a point is the point itself.
                Point point = getPointFromField<Point>(field);
                serializer.add(point);
                break;
            }
            case GeometryColumnType::Linestring: {
                LineString<Point> linestring = getLineStringFromField<Point>(field);
                computeCentroid(linestring, serializer);
                break;
            }
            case GeometryColumnType::Ring: {
                Ring<Point> ring = getRingFromField<Point>(field);
                computeCentroid(ring, serializer);
                break;
            }
            case GeometryColumnType::Polygon: {
                Polygon<Point> polygon = getPolygonFromField<Point>(field);
                computeCentroid(polygon, serializer);
                break;
            }
            case GeometryColumnType::MultiLinestring: {
                MultiLineString<Point> multilinestring = getMultiLineStringFromField<Point>(field);
                computeCentroid(multilinestring, serializer);
                break;
            }
            case GeometryColumnType::MultiPolygon: {
                MultiPolygon<Point> multipolygon = getMultiPolygonFromField<Point>(field);
                computeCentroid(multipolygon, serializer);
                break;
            }
            case GeometryColumnType::Null: {
                serializer.add(Point{0, 0});
                break;
            }
        }
    }
};

struct CartesianTraits
{
    using PointType = CartesianPoint;
    static constexpr const char * name = "centroidCartesian";
};

}

REGISTER_FUNCTION(Centroid)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates the centroid (center of mass) of a geometry in Cartesian coordinates.
Takes any geometry type and returns a Point.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "centroidCartesian(geometry)";
    FunctionDocumentation::Arguments arguments_cartesian
        = {{"geometry", "A geometry value.", {"Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point"}}};
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the centroid of the geometry as a Point.", {"Point"}};
    FunctionDocumentation::Examples examples_cartesian
        = {{"Centroid of a square",
            R"(
SELECT centroidCartesian([[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]])
        )",
            R"(
(2,2)
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {25, 7};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian
        = {description_cartesian,
           syntax_cartesian,
           arguments_cartesian,
           {},
           returned_value_cartesian,
           examples_cartesian,
           introduced_in_cartesian,
           category_cartesian};

    factory.registerFunction<FunctionCentroid<CartesianTraits>>(function_documentation_cartesian);
}

}
