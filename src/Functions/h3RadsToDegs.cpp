#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnsNumber.h>
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
extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionH3RadsToDegs final : public IFunction
{
public:
    static constexpr auto name = "h3RadsToDegs";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3RadsToDegs>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isFloat64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be Float64",
                arg->getName(), 1, getName());

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * column = checkAndGetColumn<ColumnFloat64>(arguments[0].column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be Float64",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & col_rads = column->getData();

        auto dst = ColumnVector<Float64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const Float64 rads = col_rads[row];
            auto res = radsToDegs(rads);
            dst_data[row] = res;
        }
        return dst;
    }
};

}

void registerFunctionH3RadsToDegs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3RadsToDegs>();
}

}

#endif
