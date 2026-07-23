#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <memory>
#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Point>
class FunctionPolygonPerimeter : public IFunction
{
public:
    static const char * name;

    explicit FunctionPolygonPerimeter() = default;

    static FunctionPtr create(ContextPtr)
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

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

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
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must not be Point", getName());
            else
            {
                auto geometries = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

                for (size_t i = 0; i < input_rows_count; ++i)
                    res_data.emplace_back(static_cast<Float64>(boost::geometry::perimeter(geometries[i])));
            }
        });

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

}

REGISTER_FUNCTION(PolygonPerimeter)
{
    factory.registerFunction<FunctionPolygonPerimeter<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonPerimeter<SphericalPoint>>();
}


}
