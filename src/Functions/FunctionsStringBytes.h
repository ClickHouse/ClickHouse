#pragma once

#include <cmath>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Common/BitHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
}

template <typename Impl, typename Name>
class FunctionStringBytes : public IFunction
{
public:
    static constexpr auto name = Name::name;
    using ResultType = typename Impl::ResultType;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStringBytes>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[0].type->getName(),
                getName());

        if constexpr (std::is_same_v<ResultType, UInt8>)
            return std::make_shared<DataTypeUInt8>();
        else if constexpr (std::is_same_v<ResultType, Float64>)
            return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnString * col_str = checkAndGetColumn<ColumnString>(column.get());

        if (!col_str)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());

        auto col_res = ColumnVector<ResultType>::create();
        auto & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        const ColumnString::Chars & data = col_str->getChars();
        const ColumnString::Offsets & offsets = col_str->getOffsets();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * str = reinterpret_cast<const char *>(data.data() + (i == 0 ? 0 : offsets[i - 1]));
            const size_t size = offsets[i] - (i == 0 ? 0 : offsets[i - 1]) - 1;

            vec_res[i] = Impl::process(str, size);
        }

        return col_res;
    }
};

}
