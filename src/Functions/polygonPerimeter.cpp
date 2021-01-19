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

template <typename Point>
class FunctionPolygonPerimeter : public IFunction
{
public:
    static const char * name;

    explicit FunctionPolygonPerimeter() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonPerimeter>();
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
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto parser = makeGeometryFromColumnParser<Point>(arguments[0]);
        auto container = createContainer(parser);

        auto res_column = ColumnFloat64::create();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            get(parser, container, i);

            Float64 perimeter = boost::geometry::perimeter(
                boost::get<MultiPolygon<Point>>(container));

            res_column->insertValue(perimeter);
        }

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonPerimeter<CartesianPoint>::name = "polygonPerimeterCartesian";

template <>
const char * FunctionPolygonPerimeter<GeographicPoint>::name = "polygonPerimeterGeographic";


void registerFunctionPolygonPerimeter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonPerimeter<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonPerimeter<GeographicPoint>>();
}


}
