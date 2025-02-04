#pragma once

#include <Functions/IFunction.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class FunctionToNullable : public IFunction
{
public:
    static constexpr auto name = "toNullable";

    static FunctionPtr create(ContextPtr context);

    std::string getName() const override;
    size_t getNumberOfArguments() const override;
    bool useDefaultImplementationForNulls() const override;
    bool useDefaultImplementationForNothing() const override;
    bool useDefaultImplementationForConstants() const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override;

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

}
