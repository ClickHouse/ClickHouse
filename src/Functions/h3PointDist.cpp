#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
}

namespace
{
template <class Impl>
class FunctionH3PointDist final : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static constexpr auto function = Impl::function;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3PointDist>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 4; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const auto * arg = arguments[i].get();
            if (!WhichDataType(arg).isFloat64())
                throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function {}. Must be Float64",
                        arg->getName(), i, getName());
        }
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_lat1 = checkAndGetColumn<ColumnFloat64>(non_const_arguments[0].column.get());
        if (!col_lat1)
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of argument {} of function {}. Must be Float64",
                    arguments[0].type->getName(),
                    1,
                    getName());
        const auto & data_lat1 = col_lat1->getData();

        const auto * col_lon1 = checkAndGetColumn<ColumnFloat64>(non_const_arguments[1].column.get());
        if (!col_lon1)
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of argument {} of function {}. Must be Float64",
                    arguments[1].type->getName(),
                    2,
                    getName());
        const auto & data_lon1 = col_lon1->getData();

        const auto * col_lat2 = checkAndGetColumn<ColumnFloat64>(non_const_arguments[2].column.get());
        if (!col_lat2)
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of argument {} of function {}. Must be Float64",
                    arguments[2].type->getName(),
                    3,
                    getName());
        const auto & data_lat2 = col_lat2->getData();

        const auto * col_lon2 = checkAndGetColumn<ColumnFloat64>(non_const_arguments[3].column.get());
        if (!col_lon2)
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of argument {} of function {}. Must be Float64",
                    arguments[3].type->getName(),
                    4,
                    getName());
        const auto & data_lon2 = col_lon2->getData();

        auto dst = ColumnVector<Float64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const double lat1 = data_lat1[row];
            const double lon1 = data_lon1[row];
            const auto lat2 = data_lat2[row];
            const auto lon2 = data_lon2[row];

            LatLng point1 = {degsToRads(lat1), degsToRads(lon1)};
            LatLng point2 = {degsToRads(lat2), degsToRads(lon2)};

            // function will be equivalent to distanceM or distanceKm or distanceRads
            Float64 res = function(&point1, &point2);
            dst_data[row] = res;
        }

        return dst;
    }
};

}

struct H3PointDistM
{
    static constexpr auto name = "h3PointDistM";
    static constexpr auto function = distanceM;
};

struct H3PointDistKm
{
    static constexpr auto name = "h3PointDistKm";
    static constexpr auto function = distanceKm;
};

struct H3PointDistRads
{
    static constexpr auto name = "h3PointDistRads";
    static constexpr auto function = distanceRads;
};


void registerFunctionH3PointDistM(FunctionFactory & factory) { factory.registerFunction<FunctionH3PointDist<H3PointDistM>>(); }
void registerFunctionH3PointDistKm(FunctionFactory & factory) { factory.registerFunction<FunctionH3PointDist<H3PointDistKm>>(); }
void registerFunctionH3PointDistRads(FunctionFactory & factory) { factory.registerFunction<FunctionH3PointDist<H3PointDistRads>>(); }

}

#endif
