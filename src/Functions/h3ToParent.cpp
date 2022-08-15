#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <common/range.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

class FunctionH3ToParent : public IFunction
{
public:
    static constexpr auto name = "h3ToParent";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3ToParent>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(1) + " of function " + getName() + ". Must be UInt64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arg = arguments[1].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(2) + " of function " + getName() + ". Must be UInt8",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_hindex = arguments[0].column.get();
        const auto * col_resolution = arguments[1].column.get();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (const auto row : collections::range(0, input_rows_count))
        {
            const UInt64 hindex = col_hindex->getUInt(row);
            const UInt8 resolution = col_resolution->getUInt(row);

            if (resolution > MAX_H3_RES)
                throw Exception("The argument 'resolution' (" + toString(resolution) + ") of function " + getName()
                    + " is out of bounds because the maximum resolution in H3 library is " + toString(MAX_H3_RES), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            UInt64 res = h3ToParent(hindex, resolution);

            dst_data[row] = res;
        }

        return dst;
    }
};

}

void registerFunctionH3ToParent(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3ToParent>();
}

}

#endif
