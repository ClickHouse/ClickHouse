#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <common/logger_useful.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeCustomGeo.h>

#include <memory>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

template <typename Point>
class FunctionPolygonConvexHull : public IFunction
{
public:
    static const char * name;

    explicit FunctionPolygonConvexHull() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonConvexHull>();
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
        return DataTypeCustomPolygonSerialization::nestedDataType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto parser = makeGeometryFromColumnParser<Point>(arguments[0]);
        auto container = createContainer(parser);

        PolygonSerializer<Point> serializer;

        for (size_t i = 0; i < input_rows_count; i++)
        {
            get(parser, container, i);

            Geometry<Point> convex_hull = Polygon<Point>({{{}}});
            boost::geometry::convex_hull(
                boost::get<MultiPolygon<Point>>(container),
                boost::get<Polygon<Point>>(convex_hull));

            boost::get<Polygon<Point>>(convex_hull).outer().erase(
                boost::get<Polygon<Point>>(convex_hull).outer().begin());

            serializer.add(convex_hull);
        }

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


template <>
const char * FunctionPolygonConvexHull<CartesianPoint>::name = "polygonConvexHullCartesian";

// template <>
// const char * FunctionPolygonConvexHull<GeographicPoint>::name = "polygonConvexHullGeographic";


void registerFunctionPolygonConvexHull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonConvexHull<CartesianPoint>>();
    // factory.registerFunction<FunctionPolygonConvexHull<GeographicPoint>>();
}

}
