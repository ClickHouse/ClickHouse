#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

#include <h3api.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionH3DegsToRads : public IFunction
{
public:
    static constexpr auto name = "h3DegsToRads";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3DegsToRads>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

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

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_degrees = arguments[0].column.get();

        auto dst = ColumnVector<Float64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);


        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const Float64 degrees = col_degrees->getUInt(row);
            auto res = degsToRads(degrees);
            dst_data[row] = res;
        }
        return dst;
    }
};

}

void registerFunctionH3DegsToRads(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3DegsToRads>();
}

}

#endif
