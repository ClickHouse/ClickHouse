#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <base/range.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{
    class FunctionH3ToCenterChild : public IFunction
    {
    public:
        static constexpr auto name = "h3ToCenterChild";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3ToCenterChild>(); }

        std::string getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 2; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        arg = arguments[1].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt8",
                arg->getName(), 2, getName());

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_hindex = arguments[0].column.get();
        const auto * col_resolution = arguments[1].column.get();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 hindex = col_hindex->getUInt(row);
            const UInt8 child_resolution = col_resolution->getUInt(row);

            if (child_resolution > MAX_H3_RES)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is {}",
                    toString(child_resolution), getName(), toString(MAX_H3_RES));

            UInt64  res = cellToCenterChild(hindex, child_resolution);

            dst_data[row] = res;
        }

        return dst;
    }
};

}

void registerFunctionH3ToCenterChild(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3ToCenterChild>();
}

}

#endif
