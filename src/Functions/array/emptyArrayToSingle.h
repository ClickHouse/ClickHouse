#pragma once
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/** emptyArrayToSingle(arr) - replace empty arrays with arrays of one element with a default value.
  */
class FunctionEmptyArrayToSingle : public IFunction
{
public:
    static constexpr auto name = "emptyArrayToSingle";
    static FunctionPtr createImpl() { return std::make_shared<FunctionEmptyArrayToSingle>(); }
    static FunctionPtr create(ContextPtr) { return createImpl(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

}
