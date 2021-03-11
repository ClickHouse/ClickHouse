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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


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
        auto res_column = ColumnFloat64::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        callOnGeometryDataType<Point>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            if constexpr (std::is_same_v<ColumnToPointsConverter<Point>, Converter>)
                throw Exception(fmt::format("The argument of function {} must not be Point", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            else
            {
                auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                for (size_t i = 0; i < input_rows_count; i++)
                    res_data.emplace_back(boost::geometry::perimeter(geometries[i]));
            }
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
const char * FunctionPolygonPerimeter<CartesianPoint>::name = "polygonPerimeterCartesian";

template <>
const char * FunctionPolygonPerimeter<SphericalPoint>::name = "polygonPerimeterSpherical";


void registerFunctionPolygonPerimeter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonPerimeter<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonPerimeter<SphericalPoint>>();
}


}
