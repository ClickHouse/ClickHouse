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
    extern const int BAD_ARGUMENTS;
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

    void checkInputType(const ColumnsWithTypeAndName & arguments) const
    {
        /// Array(Array(Array(Tuple(Float64, Float64))))
        auto desired = std::make_shared<const DataTypeArray>(
            std::make_shared<const DataTypeArray>(
                std::make_shared<const DataTypeArray>(
                    std::make_shared<const DataTypeTuple>(
                        DataTypes{std::make_shared<const DataTypeFloat64>(), std::make_shared<const DataTypeFloat64>()}
                    )
                )
            )
        );
        if (!desired->equals(*arguments[0].type))
            throw Exception(fmt::format("The type of the argument of function {} must be Array(Array(Array(Tuple(Float64, Float64))))", name), ErrorCodes::BAD_ARGUMENTS);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        checkInputType(arguments);
        auto parser = makeGeometryFromColumnParser<Point>(arguments[0]);
        auto container = createContainer(parser);

        PolygonSerializer<Point> serializer;

        for (size_t i = 0; i < input_rows_count; i++)
        {
            get(parser, container, i);

            auto convex_hull = Polygon<Point>({{{}}});

            boost::geometry::convex_hull(
                boost::get<MultiPolygon<Point>>(container),
                convex_hull);

            convex_hull.outer().erase(convex_hull.outer().begin());

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


void registerFunctionPolygonConvexHull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonConvexHull<CartesianPoint>>();
}

}
