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
        auto res_column = ColumnFloat64::create();

        callOnGeometryDataType<Point>(arguments[0].type, [&] (const auto & type)
        {
            using TypeParser = std::decay_t<decltype(type)>;
            // using Parser = TypeParser::Type;
            TypeParser parser(arguments[0].column->convertToFullColumnIfConst());
            auto figures = parser.parse();

            auto & res_data = res_column->getData();
            res_data.reserve(input_rows_count);

            for (size_t i = 0; i < input_rows_count; i++)
                res_data.emplace_back(boost::geometry::area(figures[i]));
        }
        );

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
