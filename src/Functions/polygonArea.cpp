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
class FunctionPolygonArea : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonArea() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonArea>();
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
        checkColumnTypeOrThrow<Point, MultiPolygon>(arguments[0]);
        auto parser = MultiPolygonFromColumnParser<Point>(std::move(arguments[0].column->convertToFullColumnIfConst()));
        MultiPolygon<Point> container;

        auto res_column = ColumnFloat64::create();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            parser.get(container, i);
            res_column->insertValue(boost::geometry::area(container));
        }

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonArea<CartesianPoint>::name = "polygonAreaCartesian";

template <>
const char * FunctionPolygonArea<GeographicPoint>::name = "polygonAreaGeographic";


void registerFunctionPolygonArea(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonArea<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonArea<GeographicPoint>>();
}


}
