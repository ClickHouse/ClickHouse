#pragma once
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/** emptyArrayToSingle(arr) - replace empty arrays with arrays of one element with a default value.
  */
class FunctionEmptyArrayToSingle final : public IFunction
{
public:
    static constexpr auto name = "emptyArrayToSingle";
    static FunctionPtr createImpl() { return std::make_shared<FunctionEmptyArrayToSingle>(); }
    static FunctionPtr create(ContextPtr) { return createImpl(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    [[nodiscard]] String getSignatureString() const override { return "(Array(T)) -> Array(T)"; }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

}
