#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <base/logger_useful.h>

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

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Point>
class FunctionPolygonsDistance : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonsDistance() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonsDistance>();
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
        return std::make_shared<DataTypeFloat64>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnFloat64::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        callOnTwoGeometryDataTypes<Point>(arguments[0].type, arguments[1].type, [&](const auto & left_type, const auto & right_type)
        {
            using LeftConverterType = std::decay_t<decltype(left_type)>;
            using RightConverterType = std::decay_t<decltype(right_type)>;

            using LeftConverter = typename LeftConverterType::Type;
            using RightConverter = typename RightConverterType::Type;

            if constexpr (std::is_same_v<ColumnToPointsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToPointsConverter<Point>, RightConverter>)
                throw Exception(fmt::format("Any argument of function {} must not be Point", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            else
            {
                auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    res_data.emplace_back(boost::geometry::distance(first[i], second[i]));
                }
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
const char * FunctionPolygonsDistance<CartesianPoint>::name = "polygonsDistanceCartesian";

template <>
const char * FunctionPolygonsDistance<SphericalPoint>::name = "polygonsDistanceSpherical";


void registerFunctionPolygonsDistance(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsDistance<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonsDistance<SphericalPoint>>();
}


}
