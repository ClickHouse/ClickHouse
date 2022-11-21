#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

namespace DB {

class ParseKeyValue : public IFunction
{
public:
    ParseKeyValue();

    static constexpr auto name = "parseKeyValue";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<ParseKeyValue>();
    }

    /// Get the main function name.
    String getName() const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override;

    bool isVariadic() const override;

    size_t getNumberOfArguments() const override;

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override;

private:
    DataTypePtr return_type;
};

}
