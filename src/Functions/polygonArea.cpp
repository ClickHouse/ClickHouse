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
            throw Exception(fmt::format("The type of first argument of function {} must be Array(Array(Array(Tuple(Float64, Float64))))", name), ErrorCodes::BAD_ARGUMENTS);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        checkInputType(arguments);
        auto parser = makeGeometryFromColumnParser<Point>(arguments[0]);

        std::cout << arguments[0].type->getName() << std::endl;
        auto container = createContainer(parser);

        auto res_column = ColumnFloat64::create();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            get(parser, container, i);

            Float64 area = boost::geometry::area(
                boost::get<MultiPolygon<Point>>(container));

            res_column->insertValue(area);
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
