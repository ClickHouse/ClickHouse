#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <common/logger_useful.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeCustomGeo.h>

#include <memory>
#include <utility>

namespace DB
{

template <typename Point>
class FunctionPolygonsWithin : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonsWithin() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonsWithin>();
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
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto first_parser = makeGeometryFromColumnParser<Point>(arguments[0]);
        auto first_container = createContainer(first_parser);

        auto second_parser = makeGeometryFromColumnParser<Point>(arguments[1]);
        auto second_container = createContainer(second_parser);

        auto res_column = ColumnUInt8::create();

        /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
        for (size_t i = 0; i < input_rows_count; i++)
        {
            get<Point>(first_parser, first_container, i);
            get<Point>(second_parser, second_container, i);

            auto first = boost::get<MultiPolygon<Point>>(first_container);
            auto second = boost::get<MultiPolygon<Point>>(second_container);

            boost::geometry::correct(first);
            boost::geometry::correct(second);

            bool within = boost::geometry::within(first, second);

            res_column->insertValue(within);
        }

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


template <>
const char * FunctionPolygonsWithin<CartesianPoint>::name = "polygonsWithinCartesian";

template <>
const char * FunctionPolygonsWithin<GeographicPoint>::name = "polygonsWithinGeographic";


void registerFunctionPolygonsWithin(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsWithin<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonsWithin<GeographicPoint>>();
}

}
